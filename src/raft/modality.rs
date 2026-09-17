//! Bounded, authenticated modality commands carried through the Raft log.

use serde::{Deserialize, Serialize};

use crate::protocol::Method;
#[cfg(feature = "modality-serving")]
use crate::server::handlers::modality::MAX_INGEST_STREAM_ITEMS;

#[cfg(feature = "modality-serving")]
const MAX_REPLICATED_MODALITY_STATE_BYTES: usize = 128 * 1024 * 1024;

#[cfg(feature = "modality-serving")]
const MAX_REPLICATED_MODALITY_RESULT_BYTES: usize = 4 * 1024;
/// MessagePack values one named `ApplyOutcome` occupies: its map, three keys,
/// and three scalar values. The structural preflight budgets every value, so a
/// stream result's budget is per outcome, not per stream item.
#[cfg(feature = "modality-serving")]
const APPLY_OUTCOME_MSGPACK_VALUES: usize = 7;
/// Value budget for one decoded result: the outcome list (or single outcome)
/// plus its enclosing array, for the largest admitted ingest stream.
#[cfg(feature = "modality-serving")]
const MAX_REPLICATED_MODALITY_RESULT_VALUES: usize =
    1 + MAX_INGEST_STREAM_ITEMS * APPLY_OUTCOME_MSGPACK_VALUES;
/// Bytes the typed header of the canonical [`SanitizedModalityResult`]
/// encoding adds beyond the wire result: its five field names plus the longest
/// schema version, modality, operation, and kind values (77 bytes for a
/// `document` `ingest_stream` `stream`), rounded up. The canonical form holds
/// the same outcome list as the wire form, so a result the wire bound admits
/// always fits this bound.
#[cfg(feature = "modality-serving")]
const MAX_CANONICAL_MODALITY_RESULT_HEADER_BYTES: usize = 128;
#[cfg(feature = "modality-serving")]
const SANITIZED_MODALITY_CODEC_VERSION: u16 = 2;

#[cfg(feature = "modality-serving")]
fn deserialize_bounded_modality_outcomes<'de, D>(
    deserializer: D,
) -> Result<Vec<eg_modality::ApplyOutcome>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoundedOutcomes;

    impl<'de> serde::de::Visitor<'de> for BoundedOutcomes {
        type Value = Vec<eg_modality::ApplyOutcome>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded sequence of modality outcomes")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut outcomes = Vec::new();
            while let Some(outcome) = sequence.next_element()? {
                if outcomes.len() >= MAX_INGEST_STREAM_ITEMS {
                    return Err(serde::de::Error::custom(
                        "sanitized modality result cardinality is outside bounds",
                    ));
                }
                outcomes.push(outcome);
            }
            Ok(outcomes)
        }
    }

    deserializer.deserialize_seq(BoundedOutcomes)
}

/// Mutation category retained by the sanitized command. It is sufficient for CDC
/// classification but contains no occurrence, source, tenant, user, or endpoint.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SanitizedModalityMutation {
    Ingest,
    IngestStream,
    Delete,
    MoveToCold,
    Restore,
    CollectTombstones,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityMutation {
    pub(crate) fn from_served(
        op: &eg_types::ServedModalityOp,
    ) -> Option<(Self, eg_types::ServedModalityKind)> {
        use eg_types::ServedModalityOp;
        match op {
            ServedModalityOp::Ingest { modality, .. } => Some((Self::Ingest, *modality)),
            ServedModalityOp::IngestStream { modality, .. } => {
                Some((Self::IngestStream, *modality))
            }
            ServedModalityOp::Delete { modality, .. } => Some((Self::Delete, *modality)),
            ServedModalityOp::MoveToCold { modality, .. } => Some((Self::MoveToCold, *modality)),
            ServedModalityOp::Restore { modality, .. } => Some((Self::Restore, *modality)),
            ServedModalityOp::CollectTombstones { modality, .. } => {
                Some((Self::CollectTombstones, *modality))
            }
            _ => None,
        }
    }

    fn wire_name(self) -> Result<String, String> {
        serde_json::to_value(self)
            .map_err(|_| "sanitized modality operation serialization failed".to_string())?
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "sanitized modality operation serialization failed".to_string())
    }
}

/// The only result schema allowed in the sanitized modality Raft command.
///
/// The public response remains the compact `ResultPayload::Raw` envelope for
/// client compatibility, but the replicated command carries this typed,
/// versioned interpretation alongside that safe response. It contains only
/// bounded outcome metadata — never source bytes, bundles, paths, or encrypted
/// runtime material. Keeping the schema here gives every replica one canonical
/// decode/validation path instead of accepting an arbitrary nested MessagePack
/// value from a leader.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SanitizedModalityResultKind {
    Single,
    Stream,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) struct SanitizedModalityResult {
    pub(crate) schema_version: u16,
    pub(crate) modality: eg_types::ServedModalityKind,
    pub(crate) operation: SanitizedModalityMutation,
    pub(crate) kind: SanitizedModalityResultKind,
    #[serde(deserialize_with = "deserialize_bounded_modality_outcomes")]
    pub(crate) outcomes: Vec<eg_modality::ApplyOutcome>,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityResult {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != SANITIZED_MODALITY_CODEC_VERSION {
            return Err("sanitized modality result schema version is unsupported".to_string());
        }
        if self.outcomes.is_empty() || self.outcomes.len() > MAX_INGEST_STREAM_ITEMS {
            return Err("sanitized modality result cardinality is outside bounds".to_string());
        }
        match (&self.kind, self.operation) {
            (SanitizedModalityResultKind::Single, operation)
                if !matches!(operation, SanitizedModalityMutation::IngestStream)
                    && self.outcomes.len() == 1 =>
            {
                Ok(())
            }
            (SanitizedModalityResultKind::Stream, SanitizedModalityMutation::IngestStream)
                if self.outcomes.len() >= 2 =>
            {
                Ok(())
            }
            _ => Err("sanitized modality result type does not match operation".to_string()),
        }
    }

    fn from_wire(
        modality: eg_types::ServedModalityKind,
        operation: SanitizedModalityMutation,
        result_msgpack: &[u8],
    ) -> Result<Self, String> {
        if result_msgpack.is_empty() || result_msgpack.len() > MAX_REPLICATED_MODALITY_RESULT_BYTES
        {
            return Err("sanitized modality Raft result is invalid".to_string());
        }
        let payload: crate::protocol::ResultPayload = eg_types::msgpack::decode_bounded(
            result_msgpack,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REPLICATED_MODALITY_RESULT_BYTES,
                MAX_REPLICATED_MODALITY_RESULT_VALUES,
                64,
            ),
        )
        .map_err(|_| "sanitized modality Raft result is malformed".to_string())?;
        // `ResultPayload` is intentionally untagged. The canonical modality
        // contract is the bounded inner outcome schema below; all non-byte
        // payloads remain invalid.
        let outcome_bytes = match payload {
            crate::protocol::ResultPayload::Raw(bytes) => bytes,
            _ => {
                return Err("sanitized modality Raft result has the wrong payload type".to_string())
            }
        };
        let (kind, outcomes) = if matches!(operation, SanitizedModalityMutation::IngestStream) {
            let outcomes: Vec<eg_modality::ApplyOutcome> = eg_types::msgpack::decode_bounded(
                &outcome_bytes,
                eg_types::msgpack::MsgpackLimits::new(
                    MAX_REPLICATED_MODALITY_RESULT_BYTES,
                    MAX_REPLICATED_MODALITY_RESULT_VALUES,
                    64,
                ),
            )
            .map_err(|_| "sanitized modality Raft stream result is malformed".to_string())?;
            (SanitizedModalityResultKind::Stream, outcomes)
        } else {
            let outcome: eg_modality::ApplyOutcome = eg_types::msgpack::decode_bounded(
                &outcome_bytes,
                eg_types::msgpack::MsgpackLimits::new(
                    MAX_REPLICATED_MODALITY_RESULT_BYTES,
                    MAX_REPLICATED_MODALITY_RESULT_VALUES,
                    64,
                ),
            )
            .map_err(|_| "sanitized modality Raft result is malformed".to_string())?;
            (SanitizedModalityResultKind::Single, vec![outcome])
        };
        let result = Self {
            schema_version: SANITIZED_MODALITY_CODEC_VERSION,
            modality,
            operation,
            kind,
            outcomes,
        };
        result.validate()?;
        if result.to_wire()?.as_slice() != result_msgpack {
            return Err("sanitized modality Raft result is not canonical".to_string());
        }
        Ok(result)
    }

    fn to_wire(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let payload = self.response_payload()?;
        rmp_serde::to_vec_named(&payload).map_err(|_| {
            "sanitized modality Raft result could not be canonically encoded".to_string()
        })
    }

    fn response_payload(&self) -> Result<crate::protocol::ResultPayload, String> {
        match self.kind {
            SanitizedModalityResultKind::Single => {
                encode_sanitized_modality_payload(&self.outcomes[0])
            }
            SanitizedModalityResultKind::Stream => {
                encode_sanitized_modality_payload(&self.outcomes)
            }
        }
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let bytes = rmp_serde::to_vec_named(self).map_err(|_| {
            "sanitized modality Raft result could not be canonically encoded".to_string()
        })?;
        if bytes.len()
            > MAX_REPLICATED_MODALITY_RESULT_BYTES + MAX_CANONICAL_MODALITY_RESULT_HEADER_BYTES
        {
            return Err("sanitized modality Raft result exceeds resource limits".to_string());
        }
        Ok(bytes)
    }
}

#[cfg(feature = "modality-serving")]
fn encode_sanitized_modality_payload<T: Serialize + ?Sized>(
    value: &T,
) -> Result<crate::protocol::ResultPayload, String> {
    rmp_serde::to_vec_named(value)
        .map(crate::protocol::ResultPayload::Raw)
        .map_err(|error| format!("result serialization failed: {error}"))
}

/// Decode the only result representation accepted for a sanitized modality
/// receipt and return the safe client payload. This is the shared authority for
/// both Raft state-machine apply and leader retry/replay; callers must not
/// reimplement the untagged `ResultPayload` or stream-cardinality checks.
#[cfg(feature = "modality-serving")]
pub(crate) fn decode_sanitized_modality_result(
    modality: eg_types::ServedModalityKind,
    operation: SanitizedModalityMutation,
    result_msgpack: &[u8],
) -> Result<crate::protocol::ResultPayload, String> {
    let result = SanitizedModalityResult::from_wire(modality, operation, result_msgpack)?;
    result.response_payload()
}

/// Request authority copied into the command authentication domain. Replicas
/// compare this binding with the surrounding `RaftRequest`, closing command
/// transplant across a tenant or either graph representation.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SanitizedModalityAuthorityBinding {
    tenant_scope: String,
    graph_name: String,
    graph_fname: String,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityAuthorityBinding {
    fn new(tenant_scope: &str, graph_name: &str, graph_fname: &str) -> Self {
        Self {
            tenant_scope: tenant_scope.to_string(),
            graph_name: graph_name.to_string(),
            graph_fname: graph_fname.to_string(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.tenant_scope.trim().is_empty()
            || self.graph_name.trim().is_empty()
            || self.graph_fname.trim().is_empty()
            || self.graph_fname != crate::persist::sanitize(&self.graph_name)
        {
            return Err("sanitized modality Raft authority binding is invalid".to_string());
        }
        Ok(())
    }
}

/// A Raft-log-safe modality command. Native decoding and policy checks happen on
/// the verified leader request; consensus receives only its existing request
/// authority, an AEAD-sealed runtime value, an opaque partition node, a small
/// non-identifying result, and integrity metadata. The raw document/media body
/// is not a field and cannot be serialized into the consensus log by this type.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedModalityRaftCommand {
    pub(crate) schema_version: u16,
    authority: SanitizedModalityAuthorityBinding,
    pub(crate) modality: eg_types::ServedModalityKind,
    pub(crate) operation: SanitizedModalityMutation,
    pub(crate) node_id: String,
    #[serde(with = "serde_bytes")]
    pub(crate) sealed_runtime_state: Vec<u8>,
    pub(crate) state_sha256: String,
    pub(crate) receipt_query: String,
    #[serde(with = "serde_bytes")]
    pub(crate) result_msgpack: Vec<u8>,
    pub(crate) result: SanitizedModalityResult,
    pub(crate) result_sha256: String,
    authentication_tag: String,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityRaftCommand {
    pub(crate) fn new(
        server_secret: &str,
        authority: (&str, &str, &str),
        kind: (eg_types::ServedModalityKind, SanitizedModalityMutation),
        node_id: String,
        sealed_runtime_state: Vec<u8>,
        receipt_query: String,
        result_msgpack: Vec<u8>,
    ) -> Result<Self, String> {
        use sha2::{Digest, Sha256};
        let (modality, operation) = kind;
        let authority =
            SanitizedModalityAuthorityBinding::new(authority.0, authority.1, authority.2);
        let state_sha256 = hex::encode(Sha256::digest(&sealed_runtime_state));
        let result = SanitizedModalityResult::from_wire(modality, operation, &result_msgpack)?;
        let result_sha256 = hex::encode(Sha256::digest(result.canonical_bytes()?));
        let authentication_tag = sanitized_modality_tag(SanitizedModalityTagInput {
            server_secret,
            schema_version: SANITIZED_MODALITY_CODEC_VERSION,
            tenant_scope: &authority.tenant_scope,
            graph_name: &authority.graph_name,
            graph_fname: &authority.graph_fname,
            modality,
            operation,
            node_id: &node_id,
            state_sha256: &state_sha256,
            receipt_query: &receipt_query,
            result_sha256: &result_sha256,
            result_msgpack: &result_msgpack,
        })?;
        let command = Self {
            schema_version: SANITIZED_MODALITY_CODEC_VERSION,
            authority,
            modality,
            operation,
            node_id,
            sealed_runtime_state,
            state_sha256,
            receipt_query,
            result_msgpack,
            result,
            result_sha256,
            authentication_tag,
        };
        command.validate(server_secret)?;
        Ok(command)
    }

    pub(crate) fn receipt_method(&self) -> Method {
        Method::ApplyMutation {
            event_type: "served_modality_v1".to_string(),
            query: self.receipt_query.clone(),
        }
    }

    fn validate(&self, server_secret: &str) -> Result<(), String> {
        self.validate_schema(server_secret)?;
        self.authority.validate()?;
        let canonical_result = self.result.canonical_bytes()?;
        self.validate_result_digest(&canonical_result)?;
        self.validate_result_wire()?;
        self.validate_runtime_state(server_secret)?;
        self.validate_partition()?;
        self.validate_receipt_query()?;
        self.validate_state_digest()?;
        self.validate_result_bytes()?;
        self.validate_authentication(server_secret)?;
        Ok(())
    }

    pub(crate) fn validate_for_request(
        &self,
        server_secret: &str,
        tenant_scope: &str,
        graph_name: &str,
        graph_fname: &str,
    ) -> Result<(), String> {
        self.validate(server_secret)?;
        if self.authority.tenant_scope != tenant_scope
            || self.authority.graph_name != graph_name
            || self.authority.graph_fname != graph_fname
        {
            return Err(
                "sanitized modality Raft authority binding does not match request".to_string(),
            );
        }
        Ok(())
    }

    fn validate_schema(&self, server_secret: &str) -> Result<(), String> {
        if server_secret.is_empty() || self.schema_version != SANITIZED_MODALITY_CODEC_VERSION {
            return Err(
                "sanitized modality Raft command schema version is unsupported".to_string(),
            );
        }
        if self.result.schema_version != self.schema_version
            || self.result.modality != self.modality
            || self.result.operation != self.operation
        {
            return Err("sanitized modality Raft result type does not match command".to_string());
        }
        self.result.validate()
    }

    fn validate_result_digest(&self, canonical_result: &[u8]) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        let observed_result = hex::encode(Sha256::digest(canonical_result));
        if observed_result != self.result_sha256
            || self.result_sha256.len() != 64
            || !self
                .result_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("sanitized modality Raft result digest does not match".to_string());
        }
        Ok(())
    }

    fn validate_result_wire(&self) -> Result<(), String> {
        if self.result.to_wire()? != self.result_msgpack {
            return Err("sanitized modality Raft result is not canonical".to_string());
        }
        Ok(())
    }

    fn validate_runtime_state(&self, server_secret: &str) -> Result<(), String> {
        if server_secret.is_empty()
            || self.sealed_runtime_state.is_empty()
            || self.sealed_runtime_state.len() > MAX_REPLICATED_MODALITY_STATE_BYTES
            || !crate::crypto::is_sealed(&self.sealed_runtime_state)
        {
            return Err("sanitized modality Raft state is invalid".to_string());
        }
        Ok(())
    }

    fn validate_partition(&self) -> Result<(), String> {
        let expected_prefix = format!(
            "__eg_internal_served_{}_",
            sanitized_modality_name(self.modality)
        );
        let Some(partition) = self.node_id.strip_prefix(&expected_prefix) else {
            return Err("sanitized modality Raft partition is invalid".to_string());
        };
        if partition.len() != 64
            || !partition
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("sanitized modality Raft partition is invalid".to_string());
        }
        Ok(())
    }

    fn validate_receipt_query(&self) -> Result<(), String> {
        if self.receipt_query.len() != 71
            || !self.receipt_query.starts_with("sha256:")
            || !self.receipt_query[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("sanitized modality Raft receipt is invalid".to_string());
        }
        Ok(())
    }

    fn validate_state_digest(&self) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        let observed_state = hex::encode(Sha256::digest(&self.sealed_runtime_state));
        if observed_state != self.state_sha256 {
            return Err("sanitized modality Raft state digest does not match".to_string());
        }
        Ok(())
    }

    fn validate_result_bytes(&self) -> Result<(), String> {
        if self.result_msgpack.is_empty()
            || self.result_msgpack.len() > MAX_REPLICATED_MODALITY_RESULT_BYTES
        {
            return Err("sanitized modality Raft result is invalid".to_string());
        }
        Ok(())
    }

    fn validate_authentication(&self, server_secret: &str) -> Result<(), String> {
        let expected_tag = sanitized_modality_tag(SanitizedModalityTagInput {
            server_secret,
            schema_version: self.schema_version,
            tenant_scope: &self.authority.tenant_scope,
            graph_name: &self.authority.graph_name,
            graph_fname: &self.authority.graph_fname,
            modality: self.modality,
            operation: self.operation,
            node_id: &self.node_id,
            state_sha256: &self.state_sha256,
            receipt_query: &self.receipt_query,
            result_sha256: &self.result_sha256,
            result_msgpack: &self.result_msgpack,
        })?;
        if !constant_time_eq(expected_tag.as_bytes(), self.authentication_tag.as_bytes()) {
            return Err("sanitized modality Raft authentication failed".to_string());
        }
        Ok(())
    }
}

#[cfg(feature = "modality-serving")]
fn sanitized_modality_name(modality: eg_types::ServedModalityKind) -> &'static str {
    match modality {
        eg_types::ServedModalityKind::Document => "document",
        eg_types::ServedModalityKind::Image => "image",
        eg_types::ServedModalityKind::Audio => "audio",
        eg_types::ServedModalityKind::Video => "video",
    }
}

#[cfg(feature = "modality-serving")]
struct SanitizedModalityTagInput<'a> {
    server_secret: &'a str,
    schema_version: u16,
    tenant_scope: &'a str,
    graph_name: &'a str,
    graph_fname: &'a str,
    modality: eg_types::ServedModalityKind,
    operation: SanitizedModalityMutation,
    node_id: &'a str,
    state_sha256: &'a str,
    receipt_query: &'a str,
    result_sha256: &'a str,
    result_msgpack: &'a [u8],
}

#[cfg(feature = "modality-serving")]
fn sanitized_modality_tag(input: SanitizedModalityTagInput<'_>) -> Result<String, String> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let operation_name = input.operation.wire_name()?;
    let mut mac = Hmac::<Sha256>::new_from_slice(input.server_secret.as_bytes())
        .map_err(|_| "sanitized modality Raft authentication failed".to_string())?;
    for value in [
        b"sanitized-modality-raft-v2".as_slice(),
        input.tenant_scope.as_bytes(),
        input.graph_name.as_bytes(),
        input.graph_fname.as_bytes(),
        sanitized_modality_name(input.modality).as_bytes(),
        operation_name.as_bytes(),
        input.node_id.as_bytes(),
        input.state_sha256.as_bytes(),
        input.receipt_query.as_bytes(),
        input.result_sha256.as_bytes(),
        input.result_msgpack,
    ] {
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value);
    }
    mac.update(&input.schema_version.to_be_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(feature = "modality-serving")]
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| {
            difference | (*left ^ *right)
        })
        == 0
}

#[cfg(all(test, feature = "modality-serving"))]
mod tests;
