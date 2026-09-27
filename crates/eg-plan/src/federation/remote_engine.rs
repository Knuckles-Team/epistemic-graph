//! Remote epistemic-graph transport and row projection.

use super::{json_to_id, ForeignSource};
use crate::rowset::RowSet;
use eg_types::wire::ForeignSourceSpec;

const MAX_REMOTE_ENGINE_ENDPOINT_BYTES: usize = 1_024;
const MAX_REMOTE_ENGINE_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_REMOTE_ENGINE_ITEMS: usize = 1_000_000;
const REMOTE_ENGINE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REMOTE_ENGINE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// ── kind (a): a remote epistemic-graph engine ──────────────────────────────────

/// Reads rows from a REMOTE epistemic-graph engine over the engine's own transport
/// (length-prefixed MessagePack + HMAC-SHA256). Borrows the spec fields (no clones).
pub struct RemoteEngineSource<'a> {
    pub(super) endpoint: &'a str,
    pub(super) graph: &'a str,
    pub(super) secret: &'a str,
    pub(super) context: &'a eg_types::acl::RequestContextClaims,
    pub(super) uql: &'a str,
    pub(super) cypher: &'a str,
    pub(super) id_field: &'a str,
}

impl ForeignSource for RemoteEngineSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        if self.uql.trim().is_empty() {
            self.fetch_cypher()
        } else {
            self.fetch_uql_text(self.uql)
        }
    }
}

/// None of `principal`/`tenant`/`audience`/`agent_id`/`policy_version` may be
/// empty (after trimming).
fn check_context_fields_nonempty(
    context: &eg_types::acl::RequestContextClaims,
) -> Result<(), String> {
    for (name, value) in [
        ("principal", context.principal.as_str()),
        ("tenant", context.tenant.as_str()),
        ("audience", context.audience.as_str()),
        ("agent_id", context.agent_id.as_str()),
        ("policy_version", context.policy_version.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!(
                "federation: remote engine v2 context {name} must not be empty"
            ));
        }
    }
    Ok(())
}

/// None of `roles`/`scopes`/`delegation` may contain an empty or duplicated
/// entry.
fn check_context_lists_valid(context: &eg_types::acl::RequestContextClaims) -> Result<(), String> {
    for (name, values) in [
        ("role", context.roles.as_slice()),
        ("scope", context.scopes.as_slice()),
        ("delegation subject", context.delegation.as_slice()),
    ] {
        let mut seen = std::collections::HashSet::new();
        if values
            .iter()
            .any(|value| value.trim().is_empty() || !seen.insert(value.as_str()))
        {
            return Err(format!(
                "federation: remote engine v2 context contains an invalid {name}"
            ));
        }
    }
    Ok(())
}

/// A non-delegated context (`principal == agent_id`) must carry an empty
/// delegation path; a delegated one must bind `principal` → … → `agent_id`
/// through at least two hops.
fn check_delegation_chain(context: &eg_types::acl::RequestContextClaims) -> Result<(), String> {
    if context.principal == context.agent_id {
        if !context.delegation.is_empty() {
            return Err(
                "federation: non-delegated remote context must have an empty delegation path"
                    .to_string(),
            );
        }
    } else if context.delegation.first() != Some(&context.principal)
        || context.delegation.last() != Some(&context.agent_id)
        || context.delegation.len() < 2
    {
        return Err(
            "federation: remote context delegation must bind principal to effective agent"
                .to_string(),
        );
    }
    Ok(())
}

/// Resolve, connect (loopback-only — native remote-engine TCP requires a
/// local verified-TLS tunnel), and configure I/O timeouts for a remote-engine
/// endpoint.
fn connect_remote_engine(endpoint: &str) -> Result<std::net::TcpStream, String> {
    use std::net::{TcpStream, ToSocketAddrs};

    if endpoint.is_empty() || endpoint.len() > MAX_REMOTE_ENGINE_ENDPOINT_BYTES {
        return Err("federation: invalid remote engine endpoint".to_string());
    }
    let addresses: Vec<_> = endpoint
        .to_socket_addrs()
        .map_err(|_| "federation: unable to resolve remote engine endpoint".to_string())?
        .take(8)
        .collect();
    if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
        return Err(
            "federation: native remote-engine TCP requires a local verified-TLS tunnel".to_string(),
        );
    }
    let mut stream = None;
    for address in addresses {
        if let Ok(candidate) = TcpStream::connect_timeout(&address, REMOTE_ENGINE_CONNECT_TIMEOUT) {
            stream = Some(candidate);
            break;
        }
    }
    let stream =
        stream.ok_or_else(|| "federation: unable to connect to remote engine".to_string())?;
    stream
        .set_read_timeout(Some(REMOTE_ENGINE_IO_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(REMOTE_ENGINE_IO_TIMEOUT)))
        .map_err(|_| "federation: unable to configure remote engine transport".to_string())?;
    Ok(stream)
}

/// Encode and write one length-prefixed request frame.
fn write_remote_request(
    stream: &mut std::net::TcpStream,
    request: &eg_types::protocol::Request,
) -> Result<(), String> {
    use std::io::Write;
    let body = rmp_serde::to_vec_named(request)
        .map_err(|_| "federation: encode request failed".to_string())?;
    if body.is_empty() || body.len() > MAX_REMOTE_ENGINE_FRAME_BYTES {
        return Err("federation: remote engine request exceeds limit".to_string());
    }
    let len = u32::try_from(body.len())
        .map_err(|_| "federation: remote engine request exceeds limit".to_string())?
        .to_be_bytes();
    stream
        .write_all(&len)
        .and_then(|()| stream.write_all(&body))
        .map_err(|_| "federation: remote engine write failed".to_string())
}

/// Read one length-prefixed response frame, bounded-decode it, and extract
/// its result bytes.
fn read_remote_response(stream: &mut std::net::TcpStream) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|_| "federation: remote engine response header failed".to_string())?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    if resp_len == 0 || resp_len > MAX_REMOTE_ENGINE_FRAME_BYTES {
        return Err("federation: remote engine response exceeds limit".to_string());
    }
    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .map_err(|_| "federation: remote engine response body failed".to_string())?;
    let resp: eg_types::protocol::Response = eg_types::msgpack::decode_bounded(
        &resp_buf,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_REMOTE_ENGINE_FRAME_BYTES,
            MAX_REMOTE_ENGINE_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "federation: invalid remote engine response".to_string())?;
    if resp.error.is_some() {
        return Err("federation: remote engine returned an error".to_string());
    }
    // `ResultPayload::raw()` is the one MessagePack-bin result representation.
    match resp.result {
        Some(eg_types::protocol::ResultPayload::Raw(bytes)) => Ok(bytes),
        _ => Err("federation: remote engine returned an unexpected result".to_string()),
    }
}

impl RemoteEngineSource<'_> {
    pub(crate) fn from_spec(spec: &ForeignSourceSpec) -> Option<RemoteEngineSource<'_>> {
        let ForeignSourceSpec::RemoteEngine {
            endpoint,
            graph,
            secret,
            context,
            uql,
            cypher,
            id_field,
        } = spec
        else {
            return None;
        };
        Some(RemoteEngineSource {
            endpoint,
            graph,
            secret,
            context,
            uql,
            cypher,
            id_field,
        })
    }

    fn validate_request_context(&self) -> Result<(), String> {
        if self.secret.is_empty() || self.secret.len() > 64 * 1024 {
            return Err(
                "federation: remote engine requires a bounded non-empty v2 signing secret"
                    .to_string(),
            );
        }
        check_context_fields_nonempty(self.context)?;
        check_context_lists_valid(self.context)?;
        check_delegation_chain(self.context)
    }

    /// Build the fail-secure `eg2.` token accepted by the remote engine. This is
    /// the same canonical v2 byte layout as `src/server/auth.rs`; v0/v1 and an
    /// empty-secret fallback are deliberately absent from native federation.
    fn auth_token(
        &self,
        request: &eg_types::protocol::Request,
        timestamp: u64,
        nonce: &str,
        idempotency_key: &str,
    ) -> Result<String, String> {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        self.validate_request_context()?;
        let method_name = request.method.tag_name();
        let body_hash = hex::encode(Sha256::digest(request.method.canonical_body_bytes()));
        let bytes = eg_types::protocol::build_envelope_v2_bytes(
            request.id,
            &request.graph,
            &method_name,
            &body_hash,
            self.context,
            timestamp,
            nonce,
            idempotency_key,
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes())
            .map_err(|_| "federation: invalid remote engine signing secret".to_string())?;
        mac.update(&bytes);
        let envelope = serde_json::json!({
            "context": self.context,
            "timestamp": timestamp,
            "nonce": nonce,
            "idempotency_key": idempotency_key,
            "mac": hex::encode(mac.finalize().into_bytes()),
        });
        let json = serde_json::to_vec(&envelope)
            .map_err(|_| "federation: encode remote engine v2 context failed".to_string())?;
        Ok(format!("eg2.{}", hex::encode(json)))
    }

    fn signed_request(
        &self,
        method: eg_types::protocol::Method,
    ) -> Result<eg_types::protocol::Request, String> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
        self.validate_request_context()?;
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "federation: system clock is before the Unix epoch".to_string())?;
        let counter = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let request_id = (elapsed.as_nanos() as u64).wrapping_add(counter).max(1);
        let nonce = format!("federation:{:032x}:{counter:016x}", elapsed.as_nanos());
        let idempotency_key = format!("federation:{request_id:016x}:{counter:016x}");
        let mut request = eg_types::protocol::Request {
            id: request_id,
            graph: self.graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(self.context.agent_id.clone()),
            method,
        };
        request.auth_token =
            self.auth_token(&request, elapsed.as_secs(), &nonce, &idempotency_key)?;
        Ok(request)
    }

    /// One framed round-trip to the remote: connect TCP, write `[u32 len][msgpack
    /// Request]`, read `[u32 len][msgpack Response]`, return the response's `Raw`
    /// payload bytes (or the remote's error). Blocking — the executor runs it on the
    /// blocking pool, exactly like the local SQL leg.
    fn round_trip(&self, request: &eg_types::protocol::Request) -> Result<Vec<u8>, String> {
        let mut stream = connect_remote_engine(self.endpoint)?;
        write_remote_request(&mut stream, request)?;
        read_remote_response(&mut stream)
    }

    /// UQL path: the remote runs the statement through its own `Method::Uql` (the one
    /// query-text surface, EH-434) and returns its rows; each row's `(id, score)` is the
    /// SAME currency this engine's plans speak, so the projection is the identity. A
    /// statement that answers no rows (`EXPLAIN`) is refused — a foreign source is rows.
    pub(crate) fn fetch_uql_text(&self, text: &str) -> Result<RowSet, String> {
        let request = self.signed_request(eg_types::protocol::Method::Uql {
            text: text.to_string(),
            params: std::collections::BTreeMap::new(),
        })?;
        let raw = self.round_trip(&request)?;
        let result: eg_types::wire::UqlResult = eg_types::msgpack::decode_bounded(
            &raw,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REMOTE_ENGINE_FRAME_BYTES,
                MAX_REMOTE_ENGINE_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "federation: invalid UQL result".to_string())?;
        let rows = match result {
            eg_types::wire::UqlResult::Rows { rows, .. }
            | eg_types::wire::UqlResult::Profile { rows, .. } => rows,
            eg_types::wire::UqlResult::Explain { .. } => {
                return Err(
                    "federation: the remote UQL statement answered no rows (EXPLAIN)".into(),
                )
            }
        };
        Ok(RowSet::from_rows(
            rows.into_iter().map(|row| (row.id, row.score)),
        ))
    }

    /// Cypher path: the remote returns a `QueryResult { columns, rows }`; pick the
    /// `id_field` column out of each row (each row is msgpack `Vec<Value>` aligned to
    /// `columns`) and build an unscored RowSet.
    fn fetch_cypher(&self) -> Result<RowSet, String> {
        let request = self.signed_request(eg_types::protocol::Method::CypherQuery {
            query: self.cypher.to_string(),
            mode: eg_types::protocol::CypherMode::Read,
        })?;
        let raw = self.round_trip(&request)?;
        let result: eg_types::protocol::QueryResult = eg_types::msgpack::decode_bounded(
            &raw,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REMOTE_ENGINE_FRAME_BYTES,
                MAX_REMOTE_ENGINE_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "federation: invalid cypher result".to_string())?;
        let id_field = if self.id_field.is_empty() {
            "id"
        } else {
            self.id_field
        };
        let col = result
            .columns
            .iter()
            .position(|c| c == id_field)
            .ok_or_else(|| {
                format!(
                    "federation: cypher result has no '{id_field}' column (have {:?})",
                    result.columns
                )
            })?;
        let mut ids = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let cells: Vec<serde_json::Value> = eg_types::msgpack::decode_bounded(
                row,
                eg_types::msgpack::MsgpackLimits::new(
                    MAX_REMOTE_ENGINE_FRAME_BYTES,
                    MAX_REMOTE_ENGINE_ITEMS,
                    eg_types::msgpack::DEFAULT_MAX_DEPTH,
                ),
            )
            .map_err(|_| "federation: invalid cypher row".to_string())?;
            if let Some(v) = cells.get(col) {
                ids.push(json_to_id(v));
            }
        }
        Ok(RowSet::from_ids(ids))
    }
}
