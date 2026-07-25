//! Query federation — EXTERNAL RowSet sources (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! A federated query reads rows from a source OUTSIDE the local engine and composes
//! them with the local graph/vector/SQL ops in ONE plan. The seam is one trait —
//! [`ForeignSource`] — that turns an external source into the SAME [`RowSet`] currency
//! every other op speaks, so a `ForeignScan` is just another leaf source (like
//! `Scan`/`Reason`/`SparqlBgp`): its rows flow straight into a downstream
//! `Filter`/`Traverse`/`Rank`/`Limit`, and a foreign∩local JOIN is the existing
//! [`RowSet::intersect_keep_order`] keyed on id.
//!
//! Two kinds implement the trait, behind one [`eg_types::wire::ForeignSourceSpec`]:
//!
//!  * [`RemoteEngineSource`] — another epistemic-graph engine reached through a LOCAL
//!    verified-TLS tunnel/sidecar over the engine's length-prefixed-MessagePack + HMAC
//!    protocol (a blocking TCP client; the executor already runs on the blocking pool).
//!    Direct routable plaintext endpoints are rejected because request authentication
//!    alone does not provide query/result confidentiality. It sends a UQL
//!    `UnifiedQueryText` (or a `CypherQuery` when no UQL is given) and projects the
//!    remote rows into a local RowSet. This composes the engine with ANOTHER engine
//!    with NO Python round-trip — the cross-engine federation seam.
//!  * [`HttpJsonSource`] — a GENERIC HTTP/JSON API: GET `url`, walk to the JSON array
//!    at `json_path`, map each element to a row via `field_map`. Any REST API becomes
//!    a joinable RowSet. Public destinations must use HTTPS. Internal destinations
//!    are fail-closed unless their exact host/origin is opted in through
//!    [`HTTP_JSON_FEDERATION_ALLOW_ENV`]; DNS is resolved once, vetted, and pinned for
//!    the request. It uses a PURE-RUST rustls HTTP client (`ureq`), never openssl —
//!    and the whole `federation` feature is kept OUT of the Pi tier.
//!
//! The whole module is gated behind `federation` (which implies `query`): a default /
//! Pi build links no ureq/rustls/ring and carries no `ForeignScan` variant.

use std::collections::HashMap;
use std::sync::Arc;

use crate::rowset::RowSet;
use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

const MAX_REMOTE_ENGINE_ENDPOINT_BYTES: usize = 1_024;
const MAX_REMOTE_ENGINE_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_REMOTE_ENGINE_ITEMS: usize = 1_000_000;
const REMOTE_ENGINE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REMOTE_ENGINE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Existing federation SSRF opt-in, shared with the server's peer fan-out. Values are
/// comma-separated exact bare hosts, `host:port` authorities, or origins. Wildcards and
/// suffix matching are deliberately unsupported. A bare host is an explicit opt-in for
/// that exact host on any port, preserving the established server-side configuration.
pub const HTTP_JSON_FEDERATION_ALLOW_ENV: &str = "EPISTEMIC_GRAPH_FEDERATION_ALLOW";

const MAX_HTTP_JSON_URL_BYTES: usize = 2 * 1024;
const MAX_HTTP_JSON_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_HTTP_JSON_ALLOWLIST_BYTES: usize = 64 * 1024;
const MAX_HTTP_JSON_ADDRESSES: usize = 8;
const MAX_HTTP_JSON_DEPTH: usize = 64;
const MAX_HTTP_JSON_ITEMS: usize = 1_000_000;
const MAX_HTTP_JSON_ROWS: usize = 250_000;
const MAX_HTTP_JSON_PATH_BYTES: usize = 4 * 1024;
const MAX_HTTP_JSON_FIELD_BYTES: usize = 1024;
const MAX_HTTP_JSON_ID_BYTES: usize = 64 * 1024;
const HTTP_JSON_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const HTTP_JSON_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const HTTP_JSON_TOTAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_BYTES: usize = 1024 * 1024;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_DSN_BYTES: usize = 8 * 1024;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_ROWS: usize = 250_000;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_FIELD_BYTES: usize = 1024;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_ID_BYTES: usize = 64 * 1024;
#[cfg(feature = "federation-sql")]
const FEDERATED_SQL_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
#[cfg(feature = "federation-sql")]
const FEDERATED_SQL_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The federation seam: turn an EXTERNAL source into the cross-modal [`RowSet`]
/// currency, so a `ForeignScan` composes with every local op. One method, one shape —
/// exactly what makes federation "just another RowSet source" rather than a bolted-on
/// second engine. CONCEPT:EG-KG.query.closure-backed-source confirms this as the trait the
/// [`ForeignSourceRegistry`] stores by name (`Arc<dyn ForeignSource + Send + Sync>`);
/// `fetch(&self)` is the `scan`-shaped method — the per-source connection spec is
/// captured in the concrete type rather than passed per call.
pub trait ForeignSource {
    /// Pull the foreign rows as a `RowSet`. A network/parse failure is an `Err` (the
    /// plan errors with a clear message rather than silently yielding nothing — a
    /// federated source being unreachable is a real error, not an empty result).
    fn fetch(&self) -> Result<RowSet, String>;
}

/// Resolve a foreign [`ForeignSourceSpec`] to its rows THROUGH the single leaf-source
/// seam (CONCEPT:EG-KG.query.symmetric-foreign-scan) — the piece that makes an
/// `Op::ForeignScan` compose EXACTLY like an internal `Op::Scan`. An internal scan's leaf
/// is `scan_label(view, label) -> RowSet`; a foreign scan's leaf is THIS fn: it produces
/// the SAME [`RowSet`] currency, so the two leaves are interchangeable through the
/// executor's `Driver` seam and any downstream `Filter`/`Traverse`/`Rank`/`Limit` sees no
/// difference between a locally-scanned source and a foreign one.
///
/// Every spec kind resolves here, symmetrically: a `Named` spec resolves BY NAME through
/// the `registry` on the `PlanCtx` (a clean typed error if none is attached), and every
/// self-describing spec (remote-engine / HTTP-JSON / external-SQL) resolves via
/// [`source_for`] + `fetch()`. The executor's `foreign_scan` arm is then a thin
/// `fuse_foreign(input, foreign_source_rows(..)?, join)` — the leaf resolve + the compose
/// — exactly mirroring the `Op::Scan` arm's thin `scan_label(..)` call.
pub fn foreign_source_rows(
    spec: &ForeignSourceSpec,
    registry: Option<&ForeignSourceRegistry>,
) -> Result<RowSet, String> {
    match spec {
        ForeignSourceSpec::Named { name } => {
            let registry = registry.ok_or_else(|| {
                format!(
                    "federation: Op::ForeignScan names foreign source '{name}' but no \
                     ForeignSourceRegistry is attached to the PlanCtx \
                     (CONCEPT:EG-KG.query.symmetric-foreign-scan)"
                )
            })?;
            registry.resolve(name)
        }
        other => source_for(other).fetch(),
    }
}

/// Build the right [`ForeignSource`] for a wire [`ForeignSourceSpec`]. The executor
/// calls this for an `Op::ForeignScan { source }` and runs `fetch()` on the blocking
/// pool, exactly like the SQL/vector legs.
pub fn source_for(spec: &ForeignSourceSpec) -> Box<dyn ForeignSource + '_> {
    match spec {
        ForeignSourceSpec::RemoteEngine {
            endpoint,
            graph,
            secret,
            context,
            uql,
            cypher,
            id_field,
        } => Box::new(RemoteEngineSource {
            endpoint,
            graph,
            secret,
            context,
            uql,
            cypher,
            id_field,
        }),
        ForeignSourceSpec::HttpJson {
            url,
            json_path,
            field_map,
        } => Box::new(HttpJsonSource {
            url,
            json_path,
            field_map,
        }),
        ForeignSourceSpec::Sql {
            dsn,
            query,
            id_field,
            score_field,
        } => sql_source(dsn, query, id_field, score_field.as_deref()),
        // CONCEPT:EG-KG.query.closure-backed-source — a `Named` spec is a REFERENCE, not a self-describing source:
        // it resolves through the executor's `ForeignSourceRegistry`, which `source_for`
        // (a pure spec→source builder with no registry) cannot reach. Hand-off is via
        // the executor / `ForeignSourceRegistry::resolve`; calling `source_for` on a
        // `Named` yields a clean error rather than a silent empty set.
        ForeignSourceSpec::Named { name } => Box::new(NamedUnresolved { name }),
    }
}

// ── kind (c): an EXTERNAL relational-SQL database (CONCEPT:EG-KG.query.feature) ─────────────

/// Build the SQL foreign source. With `federation-sql` it is the real [`SqlSource`]
/// (a pure-Rust/rustls sqlx client). WITHOUT it — a `federation`-only build — the `Sql`
/// wire variant still exists (it is pure serde, so it registers + serializes fine), but
/// there is no driver linked, so `fetch()` errors with a clear "rebuild with
/// federation-sql" message rather than panicking or silently yielding nothing.
fn sql_source<'a>(
    dsn: &'a str,
    query: &'a str,
    id_field: &'a str,
    score_field: Option<&'a str>,
) -> Box<dyn ForeignSource + 'a> {
    #[cfg(feature = "federation-sql")]
    {
        Box::new(SqlSource {
            dsn,
            query,
            id_field,
            score_field,
        })
    }
    #[cfg(not(feature = "federation-sql"))]
    {
        let _ = (dsn, query, id_field, score_field);
        Box::new(SqlUnavailable)
    }
}

/// The not-built placeholder for the `Sql` kind when `federation-sql` is off.
#[cfg(not(feature = "federation-sql"))]
struct SqlUnavailable;

#[cfg(not(feature = "federation-sql"))]
impl ForeignSource for SqlUnavailable {
    fn fetch(&self) -> Result<RowSet, String> {
        Err(
            "federation: a Sql foreign source needs a server built with the \
             `federation-sql` feature (no SQL driver in this build)"
                .into(),
        )
    }
}

// ── kind (a): a remote epistemic-graph engine ──────────────────────────────────

/// Reads rows from a REMOTE epistemic-graph engine over the engine's own transport
/// (length-prefixed MessagePack + HMAC-SHA256). Borrows the spec fields (no clones).
pub struct RemoteEngineSource<'a> {
    endpoint: &'a str,
    graph: &'a str,
    secret: &'a str,
    context: &'a eg_types::acl::RequestContextClaims,
    uql: &'a str,
    cypher: &'a str,
    id_field: &'a str,
}

impl ForeignSource for RemoteEngineSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        if self.uql.trim().is_empty() {
            self.fetch_cypher()
        } else {
            self.fetch_uql()
        }
    }
}

impl RemoteEngineSource<'_> {
    fn validate_request_context(&self) -> Result<(), String> {
        if self.secret.is_empty() || self.secret.len() > 64 * 1024 {
            return Err(
                "federation: remote engine requires a bounded non-empty v2 signing secret"
                    .to_string(),
            );
        }
        for (name, value) in [
            ("principal", self.context.principal.as_str()),
            ("tenant", self.context.tenant.as_str()),
            ("audience", self.context.audience.as_str()),
            ("agent_id", self.context.agent_id.as_str()),
            ("policy_version", self.context.policy_version.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!(
                    "federation: remote engine v2 context {name} must not be empty"
                ));
            }
        }
        for (name, values) in [
            ("role", self.context.roles.as_slice()),
            ("scope", self.context.scopes.as_slice()),
            ("delegation subject", self.context.delegation.as_slice()),
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
        if self.context.principal == self.context.agent_id {
            if !self.context.delegation.is_empty() {
                return Err(
                    "federation: non-delegated remote context must have an empty delegation path"
                        .to_string(),
                );
            }
        } else if self.context.delegation.first() != Some(&self.context.principal)
            || self.context.delegation.last() != Some(&self.context.agent_id)
            || self.context.delegation.len() < 2
        {
            return Err(
                "federation: remote context delegation must bind principal to effective agent"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Build the fail-secure `eg2.` token accepted by the remote engine. This is
    /// the same canonical v2 byte layout as `src/server/auth.rs`; v0/v1 and an
    /// empty-secret fallback are deliberately absent from native federation.
    fn auth_token_v2(
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
            self.auth_token_v2(&request, elapsed.as_secs(), &nonce, &idempotency_key)?;
        Ok(request)
    }

    /// One framed round-trip to the remote: connect TCP, write `[u32 len][msgpack
    /// Request]`, read `[u32 len][msgpack Response]`, return the response's `Raw`
    /// payload bytes (or the remote's error). Blocking — the executor runs it on the
    /// blocking pool, exactly like the local SQL leg.
    fn round_trip(&self, request: &eg_types::protocol::Request) -> Result<Vec<u8>, String> {
        use std::io::{Read, Write};
        use std::net::{TcpStream, ToSocketAddrs};

        if self.endpoint.is_empty() || self.endpoint.len() > MAX_REMOTE_ENGINE_ENDPOINT_BYTES {
            return Err("federation: invalid remote engine endpoint".to_string());
        }
        let addresses: Vec<_> = self
            .endpoint
            .to_socket_addrs()
            .map_err(|_| "federation: unable to resolve remote engine endpoint".to_string())?
            .take(8)
            .collect();
        if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
            return Err(
                "federation: native remote-engine TCP requires a local verified-TLS tunnel"
                    .to_string(),
            );
        }
        let mut stream = None;
        for address in addresses {
            if let Ok(candidate) =
                TcpStream::connect_timeout(&address, REMOTE_ENGINE_CONNECT_TIMEOUT)
            {
                stream = Some(candidate);
                break;
            }
        }
        let mut stream =
            stream.ok_or_else(|| "federation: unable to connect to remote engine".to_string())?;
        stream
            .set_read_timeout(Some(REMOTE_ENGINE_IO_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(REMOTE_ENGINE_IO_TIMEOUT)))
            .map_err(|_| "federation: unable to configure remote engine transport".to_string())?;
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
            .map_err(|_| "federation: remote engine write failed".to_string())?;

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
        // `ResultPayload::raw()` and `PropertiesMsgpack` are the SAME msgpack `bin` on
        // the wire; the untagged decoder picks `PropertiesMsgpack` (it is declared
        // first), so accept either — both carry the msgpack body we re-decode.
        match resp.result {
            Some(
                eg_types::protocol::ResultPayload::Raw(bytes)
                | eg_types::protocol::ResultPayload::PropertiesMsgpack(bytes),
            ) => Ok(bytes),
            _ => Err("federation: remote engine returned an unexpected result".to_string()),
        }
    }

    /// UQL path: the remote runs the query through its own unified planner and returns
    /// `[id, score?]` rows directly — the SAME shape this engine's `UnifiedQuery`
    /// yields, so the projection is the identity.
    fn fetch_uql(&self) -> Result<RowSet, String> {
        let request = self.signed_request(eg_types::protocol::Method::UnifiedQueryText {
            text: self.uql.to_string(),
        })?;
        let raw = self.round_trip(&request)?;
        let rows: Vec<(String, Option<f32>)> = eg_types::msgpack::decode_bounded(
            &raw,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REMOTE_ENGINE_FRAME_BYTES,
                MAX_REMOTE_ENGINE_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "federation: invalid unified rows".to_string())?;
        Ok(RowSet::from_rows(rows))
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

// ── kind (b): a generic HTTP/JSON source ───────────────────────────────────────

/// Reads rows from a generic HTTP/JSON API. Borrows the spec fields (no clones).
pub struct HttpJsonSource<'a> {
    url: &'a str,
    json_path: &'a str,
    field_map: &'a HttpFieldMap,
}

impl ForeignSource for HttpJsonSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        use std::io::Read;

        if self.json_path.len() > MAX_HTTP_JSON_PATH_BYTES
            || self.field_map.id.is_empty()
            || self.field_map.id.len() > MAX_HTTP_JSON_FIELD_BYTES
            || self
                .field_map
                .score
                .as_ref()
                .is_some_and(|field| field.len() > MAX_HTTP_JSON_FIELD_BYTES)
        {
            return Err("federation: invalid HTTP JSON projection".to_string());
        }

        // Resolve exactly once, vet every answer, then pin the resulting socket list in
        // ureq's per-call resolver. This closes the usual validate-then-resolve DNS
        // rebinding gap. Environment proxies are disabled because routing a pinned
        // request through an unvalidated implicit proxy would invalidate that guarantee.
        let target = validate_http_json_target(self.url)?;
        let pinned_addresses = target.addresses.clone();
        let agent = ureq::AgentBuilder::new()
            .try_proxy_from_env(false)
            .resolver(
                move |_: &str| -> std::io::Result<Vec<std::net::SocketAddr>> {
                    Ok(pinned_addresses.clone())
                },
            )
            .https_only(target.https_only)
            .redirects(0)
            .timeout_connect(HTTP_JSON_CONNECT_TIMEOUT)
            .timeout_read(HTTP_JSON_IO_TIMEOUT)
            .timeout_write(HTTP_JSON_IO_TIMEOUT)
            .timeout(HTTP_JSON_TOTAL_TIMEOUT)
            .build();

        let response = agent
            .get(self.url)
            .call()
            .map_err(|_| "federation: HTTP JSON request failed".to_string())?;
        if !(200..300).contains(&response.status()) {
            return Err("federation: HTTP JSON source returned an error".to_string());
        }
        if response
            .header("Content-Length")
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > MAX_HTTP_JSON_BODY_BYTES as u64)
        {
            return Err("federation: HTTP JSON response exceeds limit".to_string());
        }

        // `take(max + 1)` applies to the decoded response stream too, so a compressed
        // body cannot inflate past the cap. The extra byte distinguishes exactly-at-cap
        // from oversized without trusting Content-Length or transfer framing.
        let mut body = Vec::new();
        response
            .into_reader()
            .take((MAX_HTTP_JSON_BODY_BYTES + 1) as u64)
            .read_to_end(&mut body)
            .map_err(|_| "federation: HTTP JSON response read failed".to_string())?;
        if body.len() > MAX_HTTP_JSON_BODY_BYTES {
            return Err("federation: HTTP JSON response exceeds limit".to_string());
        }
        validate_json_shape(&body)?;
        let json: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| "federation: invalid HTTP JSON response".to_string())?;
        let array = walk_json_path(&json, self.json_path)
            .ok_or_else(|| "federation: HTTP JSON projection did not resolve".to_string())?;
        let elems = array
            .as_array()
            .ok_or_else(|| "federation: HTTP JSON projection is not an array".to_string())?;
        if elems.len() > MAX_HTTP_JSON_ROWS {
            return Err("federation: HTTP JSON row count exceeds limit".to_string());
        }

        let mut rows = Vec::with_capacity(elems.len());
        for element in elems {
            let Some(value) = element.get(&self.field_map.id) else {
                continue;
            };
            let Some(id) = bounded_json_id(value)? else {
                continue;
            };
            let score = self
                .field_map
                .score
                .as_ref()
                .and_then(|field| element.get(field))
                .and_then(|v| v.as_f64())
                .filter(|value| value.is_finite() && value.abs() <= f32::MAX as f64)
                .map(|value| value as f32);
            rows.push((id, score));
        }
        Ok(RowSet::from_rows(rows))
    }
}

/// The validated, DNS-pinned transport target. No URL/host is retained here, keeping it
/// out of `Debug`/error paths and reducing the chance that embedded query credentials are
/// reflected by a future caller.
struct ValidatedHttpJsonTarget {
    addresses: Vec<std::net::SocketAddr>,
    /// `false` is only possible for an exact-allowlisted internal HTTP target.
    https_only: bool,
}

/// Parse and validate a federation URL without reflecting it in any error. Public
/// destinations are HTTPS-only. An internal/special-use resolution is accepted only
/// when the exact host, authority, or origin is present in the established federation
/// allowlist. Explicitly allowlisted internal endpoints may use HTTP for a local trusted
/// tunnel/test service; HTTP to a public destination remains forbidden.
fn validate_http_json_target(url: &str) -> Result<ValidatedHttpJsonTarget, String> {
    use std::net::ToSocketAddrs;

    if url.is_empty()
        || url.len() > MAX_HTTP_JSON_URL_BYTES
        || url.trim() != url
        || url
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'\\')
        || url.contains('#')
    {
        return Err("federation: invalid HTTP JSON URL".to_string());
    }

    let (is_https, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest)
    } else {
        return Err("federation: HTTP JSON source requires HTTPS".to_string());
    };
    let authority = rest.split(['/', '?']).next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') || authority.contains('%') {
        return Err("federation: invalid HTTP JSON URL authority".to_string());
    }

    let default_port = if is_https { 443 } else { 80 };
    let (host, port) = parse_http_authority(authority, default_port)?;
    let host = normalize_http_host(host)?;
    let allowlisted = http_json_target_allowlisted(&host, port, is_https);

    let mut addresses: Vec<_> = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|_| "federation: unable to resolve HTTP JSON source".to_string())?
        .take(MAX_HTTP_JSON_ADDRESSES + 1)
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() || addresses.len() > MAX_HTTP_JSON_ADDRESSES {
        return Err("federation: invalid HTTP JSON source resolution".to_string());
    }

    let has_sensitive_address = addresses
        .iter()
        .any(|address| is_ssrf_sensitive_ip(address.ip()));
    if has_sensitive_address && !allowlisted {
        return Err("federation: HTTP JSON destination is not allowed".to_string());
    }
    if !(is_https || has_sensitive_address && allowlisted) {
        return Err("federation: HTTP JSON source requires HTTPS".to_string());
    }

    Ok(ValidatedHttpJsonTarget {
        addresses,
        https_only: is_https,
    })
}

fn parse_http_authority(authority: &str, default_port: u16) -> Result<(&str, u16), String> {
    if let Some(bracketed) = authority.strip_prefix('[') {
        let close = bracketed
            .find(']')
            .ok_or_else(|| "federation: invalid HTTP JSON URL authority".to_string())?;
        let host = &bracketed[..close];
        let suffix = &bracketed[close + 1..];
        let port = if suffix.is_empty() {
            default_port
        } else {
            let raw_port = suffix
                .strip_prefix(':')
                .ok_or_else(|| "federation: invalid HTTP JSON URL authority".to_string())?;
            parse_http_port(raw_port)?
        };
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return Err("federation: invalid HTTP JSON URL authority".to_string());
        }
        return Ok((host, port));
    }

    if authority.matches(':').count() > 1 {
        return Err("federation: invalid HTTP JSON URL authority".to_string());
    }
    match authority.rsplit_once(':') {
        Some((host, raw_port)) => Ok((host, parse_http_port(raw_port)?)),
        None => Ok((authority, default_port)),
    }
}

fn parse_http_port(raw: &str) -> Result<u16, String> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("federation: invalid HTTP JSON URL port".to_string());
    }
    raw.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| "federation: invalid HTTP JSON URL port".to_string())
}

fn normalize_http_host(host: &str) -> Result<String, String> {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 || !host.is_ascii() {
        return Err("federation: invalid HTTP JSON URL host".to_string());
    }
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(host);
    }
    let valid = host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    });
    if !valid {
        return Err("federation: invalid HTTP JSON URL host".to_string());
    }
    Ok(host)
}

/// Exact allowlist matching only: no substrings, suffixes, globs, regexes, or CIDRs.
fn http_json_target_allowlisted(host: &str, port: u16, is_https: bool) -> bool {
    let Ok(raw) = std::env::var(HTTP_JSON_FEDERATION_ALLOW_ENV) else {
        return false;
    };
    if raw.len() > MAX_HTTP_JSON_ALLOWLIST_BYTES {
        return false;
    }
    let scheme = if is_https { "https" } else { "http" };
    let authority_host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let authority = format!("{authority_host}:{port}");
    let origin = format!("{scheme}://{authority}");
    let scheme_host = format!("{scheme}://{authority_host}");
    raw.split(',').take(1024).any(|entry| {
        let entry = entry.trim().to_ascii_lowercase();
        entry == host
            || entry == authority_host
            || entry == authority
            || entry == scheme_host
            || entry == origin
    })
}

/// Reject every local/private/link-local/multicast/documentation/transition range that
/// can address a non-public service. Public HTTPS is allowed; an operator must explicitly
/// opt an exact host/origin into this set's exceptions.
fn is_ssrf_sensitive_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(address) => {
            let [a, b, c, _] = address.octets();
            address.is_loopback()
                || address.is_unspecified()
                || address.is_private()
                || address.is_link_local()
                || address.is_multicast()
                || address.is_broadcast()
                || address.is_documentation()
                || a == 0
                || a >= 240
                || (a == 100 && (64..=127).contains(&b)) // carrier-grade NAT
                || (a == 192 && b == 0 && c == 0) // IETF protocol assignments
                || (a == 192 && b == 88 && c == 99) // deprecated 6to4 relay anycast
                || (a == 198 && (b == 18 || b == 19)) // benchmark networks
        }
        std::net::IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4() {
                return is_ssrf_sensitive_ip(std::net::IpAddr::V4(mapped));
            }
            let segments = address.segments();
            address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
                || (segments[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (segments[0] & 0xffc0) == 0xfec0 // deprecated site-local fec0::/10
                || (segments[0] == 0x0064 && segments[1] == 0xff9b) // NAT64 translation
                || segments[0] == 0x2002 // 6to4 embeds an IPv4 target
                || (segments[0] == 0x2001 && segments[1] == 0x0000) // Teredo
                || (segments[0] == 0x2001 && segments[1] == 0x0db8) // documentation
                || (segments[0] == 0x2001 && (segments[1] & 0xfff0) == 0x0020) // ORCHID
                || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0]) // discard-only
        }
    }
}

/// Allocation-free resource preflight. `serde_json` remains the grammar authority; this
/// pass bounds nesting and aggregate structural slots before it can allocate a DOM.
fn validate_json_shape(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_HTTP_JSON_BODY_BYTES {
        return Err("federation: HTTP JSON response exceeds limit".to_string());
    }
    let mut depth = 0usize;
    let mut items = usize::from(!bytes.is_empty());
    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes.iter().copied() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                items = items.saturating_add(1);
                if depth > MAX_HTTP_JSON_DEPTH {
                    return Err("federation: HTTP JSON nesting exceeds limit".to_string());
                }
            }
            b'}' | b']' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "federation: invalid HTTP JSON response".to_string())?;
            }
            b',' => items = items.saturating_add(1),
            _ => {}
        }
        if items > MAX_HTTP_JSON_ITEMS {
            return Err("federation: HTTP JSON item count exceeds limit".to_string());
        }
    }
    if in_string || depth != 0 {
        return Err("federation: invalid HTTP JSON response".to_string());
    }
    Ok(())
}

fn bounded_json_id(value: &serde_json::Value) -> Result<Option<String>, String> {
    let id = match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            return Ok(None);
        }
    };
    if id.len() > MAX_HTTP_JSON_ID_BYTES {
        return Err("federation: HTTP JSON row identifier exceeds limit".to_string());
    }
    Ok(Some(id))
}

#[cfg(test)]
mod http_json_security_tests {
    use super::{
        is_ssrf_sensitive_ip, parse_http_authority, validate_http_json_target, validate_json_shape,
        MAX_HTTP_JSON_DEPTH,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn internal_and_transition_addresses_are_sensitive() {
        for address in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            "2001:db8::1".parse().unwrap(),
            "64:ff9b::1".parse().unwrap(),
        ] {
            assert!(is_ssrf_sensitive_ip(address));
        }
        assert!(!is_ssrf_sensitive_ip(IpAddr::V4(Ipv4Addr::new(
            93, 184, 216, 34
        ))));
    }

    #[test]
    fn authority_parser_rejects_ambiguous_or_invalid_ports() {
        assert!(parse_http_authority("::1:443", 443).is_err());
        assert!(parse_http_authority("example.invalid:0", 443).is_err());
        assert!(parse_http_authority("[::1]:443", 80).is_ok());
    }

    #[test]
    fn json_preflight_rejects_excessive_nesting() {
        let mut body = vec![b'['; MAX_HTTP_JSON_DEPTH + 1];
        body.extend(std::iter::repeat_n(b']', MAX_HTTP_JSON_DEPTH + 1));
        assert!(validate_json_shape(&body).is_err());
    }

    #[test]
    fn destination_errors_do_not_reflect_url_secrets() {
        let secret = "test-sensitive-token-value";
        let error = validate_http_json_target(&format!("ftp://example.invalid/?token={secret}"))
            .err()
            .expect("unsupported scheme must fail");
        assert!(!error.contains(secret));
        assert!(!error.contains("example.invalid"));
    }
}

// ── kind (c) impl: external relational-SQL (CONCEPT:EG-KG.query.feature, feature `federation-sql`) ─

#[cfg(feature = "federation-sql")]
#[derive(Clone, Copy)]
enum SqlDialect {
    Postgres,
    MySql,
}

/// SQLx 0.9 deliberately requires an explicit audit marker for runtime SQL. This
/// parser is that audit boundary: exactly one query is accepted, including every
/// nested statement, and locking/`SELECT INTO` write-shaped queries are rejected.
/// The database read-only transaction below remains the authoritative second layer.
#[cfg(feature = "federation-sql")]
fn validate_federated_sql(query: &str, dialect: SqlDialect) -> Result<(), String> {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Query, Select, Statement, Visit, Visitor};
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
    use sqlparser::parser::Parser;

    if query.is_empty() || query.len() > MAX_FEDERATED_SQL_BYTES || query.contains('\0') {
        return Err("federation: invalid SQL query size or encoding".to_string());
    }
    let statements = match dialect {
        SqlDialect::Postgres => Parser::parse_sql(&PostgreSqlDialect {}, query),
        SqlDialect::MySql => Parser::parse_sql(&MySqlDialect {}, query),
    }
    .map_err(|_| "federation: SQL query did not parse".to_string())?;
    if statements.len() != 1 || !matches!(statements.first(), Some(Statement::Query(_))) {
        return Err("federation: SQL source requires exactly one read query".to_string());
    }

    struct ReadOnlyVisitor;
    impl Visitor for ReadOnlyVisitor {
        type Break = ();

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
            if matches!(statement, Statement::Query(_)) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if query.locks.is_empty() {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
            if select.into.is_none() {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }

    let mut visitor = ReadOnlyVisitor;
    if statements[0].visit(&mut visitor).is_break() {
        return Err("federation: SQL query is not read-only".to_string());
    }
    Ok(())
}

/// Reads rows from an EXTERNAL relational DB (Postgres/MySQL) over a pure-Rust/rustls
/// `sqlx` client. The statement must parse as one read query, then executes inside a
/// database-enforced read-only transaction with time and cardinality bounds. The DSN scheme picks the dialect
/// (`postgres://`/`postgresql://` ⇒ Postgres, `mysql://` ⇒ MySQL); each row's `id_field`
/// column becomes the row id and the optional `score_field` becomes the row score.
///
/// `fetch()` is SYNC (the executor runs it on the blocking pool, exactly like the SQL /
/// vector legs) but `sqlx` is async, so it spins a small current-thread tokio runtime to
/// drive the connect+query to completion. A per-call connection prevents session state
/// from crossing requests.
#[cfg(feature = "federation-sql")]
pub struct SqlSource<'a> {
    dsn: &'a str,
    query: &'a str,
    id_field: &'a str,
    score_field: Option<&'a str>,
}

#[cfg(feature = "federation-sql")]
impl ForeignSource for SqlSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("federation: build tokio runtime: {e}"))?;
        rt.block_on(self.fetch_async())
    }
}

#[cfg(feature = "federation-sql")]
impl SqlSource<'_> {
    async fn fetch_async(&self) -> Result<RowSet, String> {
        if self.dsn.is_empty()
            || self.dsn.len() > MAX_FEDERATED_SQL_DSN_BYTES
            || self.dsn.contains('\0')
        {
            return Err("federation: invalid SQL connection configuration".to_string());
        }
        let scheme = self.dsn.split(':').next().unwrap_or("");
        match scheme {
            "postgres" | "postgresql" => {
                validate_federated_sql(self.query, SqlDialect::Postgres)?;
                self.fetch_postgres().await
            }
            "mysql" | "mariadb" => {
                validate_federated_sql(self.query, SqlDialect::MySql)?;
                self.fetch_mysql().await
            }
            _ => Err(
                "federation: unsupported SQL connection scheme (expected postgres or mysql)"
                    .to_string(),
            ),
        }
    }

    async fn fetch_postgres(&self) -> Result<RowSet, String> {
        use futures_util::TryStreamExt;
        use sqlx::Connection;
        self.validate_projection_fields()?;
        let mut conn = tokio::time::timeout(
            FEDERATED_SQL_CONNECT_TIMEOUT,
            sqlx::postgres::PgConnection::connect(self.dsn),
        )
        .await
        .map_err(|_| "federation: postgres connection timed out".to_string())?
        .map_err(|_| "federation: postgres connection failed".to_string())?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|_| "federation: postgres transaction failed".to_string())?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|_| "federation: postgres read-only transaction unavailable".to_string())?;
        let out = tokio::time::timeout(FEDERATED_SQL_QUERY_TIMEOUT, async {
            let mut rows = sqlx::query(sqlx::AssertSqlSafe(self.query.to_owned())).fetch(&mut *tx);
            let mut out = Vec::new();
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|_| "federation: postgres query failed".to_string())?
            {
                if out.len() >= MAX_FEDERATED_SQL_ROWS {
                    return Err("federation: postgres result exceeds row limit".to_string());
                }
                out.push(self.project_pg_row(&row)?);
            }
            Ok::<_, String>(out)
        })
        .await
        .map_err(|_| "federation: postgres query timed out".to_string())??;
        tx.rollback()
            .await
            .map_err(|_| "federation: postgres read-only transaction cleanup failed".to_string())?;
        Ok(RowSet::from_rows(out))
    }

    async fn fetch_mysql(&self) -> Result<RowSet, String> {
        use futures_util::TryStreamExt;
        use sqlx::Connection;
        self.validate_projection_fields()?;
        let mut conn = tokio::time::timeout(
            FEDERATED_SQL_CONNECT_TIMEOUT,
            sqlx::mysql::MySqlConnection::connect(self.dsn),
        )
        .await
        .map_err(|_| "federation: mysql connection timed out".to_string())?
        .map_err(|_| "federation: mysql connection failed".to_string())?;
        // MySQL applies this setting to the next transaction; the connection is
        // request-scoped and discarded immediately afterward.
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut conn)
            .await
            .map_err(|_| "federation: mysql read-only transaction unavailable".to_string())?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|_| "federation: mysql transaction failed".to_string())?;
        let out = tokio::time::timeout(FEDERATED_SQL_QUERY_TIMEOUT, async {
            let mut rows = sqlx::query(sqlx::AssertSqlSafe(self.query.to_owned())).fetch(&mut *tx);
            let mut out = Vec::new();
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|_| "federation: mysql query failed".to_string())?
            {
                if out.len() >= MAX_FEDERATED_SQL_ROWS {
                    return Err("federation: mysql result exceeds row limit".to_string());
                }
                out.push(self.project_my_row(&row)?);
            }
            Ok::<_, String>(out)
        })
        .await
        .map_err(|_| "federation: mysql query timed out".to_string())??;
        tx.rollback()
            .await
            .map_err(|_| "federation: mysql read-only transaction cleanup failed".to_string())?;
        Ok(RowSet::from_rows(out))
    }

    fn validate_projection_fields(&self) -> Result<(), String> {
        for (name, value) in [
            ("id_field", Some(self.id_field)),
            ("score_field", self.score_field),
        ] {
            if let Some(value) = value {
                if value.is_empty()
                    || value.len() > MAX_FEDERATED_SQL_FIELD_BYTES
                    || value.contains('\0')
                {
                    return Err(format!("federation: invalid {name}"));
                }
            }
        }
        Ok(())
    }

    fn project_pg_row(&self, row: &sqlx::postgres::PgRow) -> Result<(String, Option<f32>), String> {
        use sqlx::Row;
        let id = pg_col_to_id(row, self.id_field)?;
        if id.len() > MAX_FEDERATED_SQL_ID_BYTES {
            return Err("federation: postgres id exceeds size limit".to_string());
        }
        // Read the score as f64 (float8/numeric-via-double) or f32 (float4) — a NULL or a
        // non-numeric column yields no score rather than erroring (the score is optional).
        let score = self.score_field.and_then(|sf| {
            row.try_get::<f64, _>(sf)
                .map(|v| v as f32)
                .or_else(|_| row.try_get::<f32, _>(sf))
                .ok()
        });
        Ok((id, score))
    }

    fn project_my_row(&self, row: &sqlx::mysql::MySqlRow) -> Result<(String, Option<f32>), String> {
        use sqlx::Row;
        let id = my_col_to_id(row, self.id_field)?;
        if id.len() > MAX_FEDERATED_SQL_ID_BYTES {
            return Err("federation: mysql id exceeds size limit".to_string());
        }
        let score = self.score_field.and_then(|sf| {
            row.try_get::<f64, _>(sf)
                .map(|v| v as f32)
                .or_else(|_| row.try_get::<f32, _>(sf))
                .ok()
        });
        Ok((id, score))
    }
}

/// Read the `id_field` column of a Postgres row as a String id, trying the common id
/// SQL types in order (text, then integer, then float). A column that decodes as none of
/// these errors clearly (rather than silently dropping the row).
#[cfg(feature = "federation-sql")]
fn pg_col_to_id(row: &sqlx::postgres::PgRow, col: &str) -> Result<String, String> {
    use sqlx::Row;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return Ok(s);
    }
    if let Ok(n) = row.try_get::<i64, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(n) = row.try_get::<i32, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(f) = row.try_get::<f64, _>(col) {
        return Ok(f.to_string());
    }
    Err(format!(
        "federation: id column '{col}' is not a string/int/float (cast it to text in the query)"
    ))
}

/// MySQL counterpart of [`pg_col_to_id`].
#[cfg(feature = "federation-sql")]
fn my_col_to_id(row: &sqlx::mysql::MySqlRow, col: &str) -> Result<String, String> {
    use sqlx::Row;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return Ok(s);
    }
    if let Ok(n) = row.try_get::<i64, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(n) = row.try_get::<i32, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(f) = row.try_get::<f64, _>(col) {
        return Ok(f.to_string());
    }
    Err(format!(
        "federation: id column '{col}' is not a string/int/float (cast it to text in the query)"
    ))
}

// ── shared helpers ─────────────────────────────────────────────────────────────

/// Walk a dotted JSON path (e.g. `data.items`) into `root`. An empty path returns the
/// root unchanged (the response IS the array). A missing segment ⇒ `None`.
fn walk_json_path<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = root;
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Stringify a JSON scalar to a row id. A JSON string is used verbatim (no quotes); any
/// other scalar is rendered without surrounding quotes (e.g. a number `42` → "42").
fn json_to_id(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ── CONCEPT:EG-KG.query.closure-backed-source — the foreign-source NAME REGISTRY + registerable source kinds ─

/// A boxed, thread-safe [`ForeignSource`] stored by name in a [`ForeignSourceRegistry`]
/// (CONCEPT:EG-KG.query.closure-backed-source). It is `Send + Sync` (the registry is shared across the executor's
/// blocking pool) and OWNS its data (`'static`) — unlike the borrow-based sources
/// [`source_for`] builds per-op straight off a wire spec.
pub type SharedForeignSource = Arc<dyn ForeignSource + Send + Sync>;

/// CONCEPT:EG-KG.query.closure-backed-source — the federation SOURCE REGISTRY: maps a foreign-source NAME to a live
/// [`ForeignSource`]. This is the resolution seam the UQL `FOREIGN "<name>"` clause
/// (`Op::Foreign`) and a `Named` [`eg_types::wire::Op::ForeignScan`] resolve through —
/// the piece the wire doc-comment flagged as "the server-side foreign_sources registry
/// that eg-plan (below the server) cannot reach". It now lives IN eg-plan and threads
/// into the executor via `PlanCtx::with_foreign`, so a name → rows resolution needs no
/// per-op inline spec and no Python round-trip.
///
/// A default `PlanCtx` carries no registry. Executing a name-resolving foreign op in
/// that context is a typed error, so a missing binding cannot silently leak local rows.
#[derive(Default, Clone)]
pub struct ForeignSourceRegistry {
    sources: HashMap<String, SharedForeignSource>,
}

impl ForeignSourceRegistry {
    /// A new, empty registry (no foreign sources bound). CONCEPT:EG-KG.query.closure-backed-source.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a source under `name`. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register(&mut self, name: impl Into<String>, source: SharedForeignSource) -> &mut Self {
        self.sources.insert(name.into(), source);
        self
    }

    /// Register an owned [`ForeignSourceSpec`] (remote-engine / HTTP-JSON / SQL) under a
    /// name — the kind that "resolves the name to another graph/dataset": a
    /// `RemoteEngine` spec pointed at another graph is exactly that, reached over the
    /// engine's own transport. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register_spec(&mut self, name: impl Into<String>, spec: ForeignSourceSpec) -> &mut Self {
        self.register(name, Arc::new(SpecSource { spec }))
    }

    /// Register a FIXED table of rows (id + optional score) under a name — the
    /// zero-dependency source kind for tests + pre-materialized datasets.
    /// CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register_table<I>(&mut self, name: impl Into<String>, rows: I) -> &mut Self
    where
        I: IntoIterator<Item = (String, Option<f32>)>,
    {
        self.register(
            name,
            Arc::new(TableSource {
                rows: rows.into_iter().collect(),
            }),
        )
    }

    /// Register a CLOSURE that produces the rows on demand under a name. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register_closure<F>(&mut self, name: impl Into<String>, f: F) -> &mut Self
    where
        F: Fn() -> Result<RowSet, String> + Send + Sync + 'static,
    {
        self.register(name, Arc::new(ClosureSource { f: Box::new(f) }))
    }

    /// Look a source up by name (borrowing the shared handle). CONCEPT:EG-KG.query.closure-backed-source.
    pub fn get(&self, name: &str) -> Option<&SharedForeignSource> {
        self.sources.get(name)
    }

    /// How many sources are registered.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Whether NO sources are registered.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Resolve `name` to its foreign rows, or a CLEAN typed error naming the unbound
    /// source (and listing what IS registered). This is what the executor calls for a
    /// `Named` `Op::ForeignScan` / an `Op::Foreign` marker. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn resolve(&self, name: &str) -> Result<RowSet, String> {
        match self.sources.get(name) {
            Some(src) => src.fetch(),
            None => {
                let mut known: Vec<&str> = self.sources.keys().map(String::as_str).collect();
                known.sort_unstable();
                Err(format!(
                    "federation: no foreign source registered under name '{name}' \
                     (registered: {known:?}) (CONCEPT:EG-KG.query.closure-backed-source)"
                ))
            }
        }
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — a registerable source backed by an owned [`ForeignSourceSpec`]. It
/// delegates to [`source_for`], so the SAME remote-engine / HTTP-JSON / external-SQL
/// machinery becomes name-addressable. A `RemoteEngine` spec pointed at another graph is
/// the "in-engine source that resolves a name to another graph/dataset" kind. (A `Named`
/// spec here would recurse into `source_for`'s clean error rather than loop.)
pub struct SpecSource {
    spec: ForeignSourceSpec,
}

impl ForeignSource for SpecSource {
    fn fetch(&self) -> Result<RowSet, String> {
        source_for(&self.spec).fetch()
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — a registerable source backed by a FIXED table of rows (id + optional
/// score). The zero-dependency kind for tests and pre-materialized foreign datasets.
pub struct TableSource {
    rows: Vec<(String, Option<f32>)>,
}

impl ForeignSource for TableSource {
    fn fetch(&self) -> Result<RowSet, String> {
        Ok(RowSet::from_rows(self.rows.iter().cloned()))
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — a registerable source backed by a CLOSURE producing rows on demand
/// (e.g. an in-engine adapter that reads from another dataset the host holds).
pub struct ClosureSource {
    f: Box<dyn Fn() -> Result<RowSet, String> + Send + Sync>,
}

impl ForeignSource for ClosureSource {
    fn fetch(&self) -> Result<RowSet, String> {
        (self.f)()
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — the placeholder [`ForeignSource`] a `Named` spec resolves to when it
/// reaches [`source_for`] (which has no registry). It always errors, pointing the caller
/// at the `ForeignSourceRegistry`, so a misrouted `Named` fails loudly, never silently.
struct NamedUnresolved<'a> {
    name: &'a str,
}

impl ForeignSource for NamedUnresolved<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        Err(format!(
            "federation: the Named foreign source '{}' resolves through the \
             ForeignSourceRegistry on the PlanCtx, not source_for (CONCEPT:EG-KG.query.closure-backed-source)",
            self.name
        ))
    }
}

#[cfg(test)]
mod symmetric_scan_oracle {
    //! Compose-oracle (CONCEPT:EG-KG.query.symmetric-foreign-scan): a `ForeignScan` leaf
    //! composes through the executor's `Driver` seam EXACTLY like an internal `Scan` leaf.
    //! Proof: register a foreign source returning EXACTLY the rows an internal
    //! `Scan("Doc")` seeds, then run TWO plans that differ ONLY in the leaf op
    //! (`Scan` vs `ForeignScan{join:false}`) under the SAME downstream
    //! `Filter -> Rank -> Limit`. The two results MUST be byte-identical — the seam does
    //! not distinguish a locally-scanned source from a foreign one.
    use crate::algebra::Op;
    use crate::exec::{execute, PlanCtx};
    use crate::federation::ForeignSourceRegistry;
    use crate::rowset::Row;
    use crate::Plan;
    use eg_types::wire::{ForeignSourceSpec, Pred};

    /// The identical downstream applied to BOTH the internal and foreign leaf.
    fn downstream() -> Vec<Op> {
        vec![
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2023.0,
                }],
            },
            Op::Rank {
                query: crate::fixture::query_vec(),
            },
            Op::Limit { k: 10 },
        ]
    }

    fn as_pairs(rows: &[Row]) -> Vec<(String, Option<f32>)> {
        rows.iter().map(|r| (r.id.clone(), r.score)).collect()
    }

    #[test]
    fn foreign_scan_composes_exactly_like_internal_scan() {
        let fx = crate::fixture::build();

        // Capture the EXACT RowSet an internal `Scan("Doc")` leaf seeds.
        let ctx_plain = PlanCtx::new(&fx.view, &fx.semantic);
        let scanned = execute(
            &Plan::new(vec![Op::Scan {
                label: "Doc".into(),
            }]),
            &ctx_plain,
        )
        .unwrap();
        let scanned_rows = as_pairs(scanned.rows());

        // A foreign source that returns EXACTLY those rows — the symmetric mirror of Scan.
        let mut registry = ForeignSourceRegistry::new();
        registry.register_table("mirror-doc", scanned_rows);

        // Two plans, identical but for the leaf op.
        let mut internal_ops = vec![Op::Scan {
            label: "Doc".into(),
        }];
        internal_ops.extend(downstream());
        let mut foreign_ops = vec![Op::ForeignScan {
            source: Box::new(ForeignSourceSpec::Named {
                name: "mirror-doc".into(),
            }),
            join: false,
        }];
        foreign_ops.extend(downstream());

        let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
        let internal = execute(&Plan::new(internal_ops), &ctx).unwrap();
        let foreign = execute(&Plan::new(foreign_ops), &ctx).unwrap();

        assert_eq!(
            as_pairs(foreign.rows()),
            as_pairs(internal.rows()),
            "a ForeignScan leaf composes byte-identically to an internal Scan leaf"
        );
        // And the composition actually did something (guards a vacuous pass).
        assert!(!internal.ids().is_empty(), "downstream produced rows");
    }
}

#[cfg(test)]
mod envelope_signer_tests {
    //! Native federation must use the same verified v2 identity/policy envelope
    //! as every other served client. These tests pin its canonical bindings and
    //! prove the former v0/empty-secret downgrade no longer exists.
    use super::RemoteEngineSource;
    use eg_types::acl::RequestContextClaims;
    use eg_types::protocol::{Method, Request};

    fn context() -> RequestContextClaims {
        RequestContextClaims {
            principal: "agent:federation".into(),
            tenant: "tenant-a".into(),
            audience: "epistemic-graph".into(),
            agent_id: "agent:federation".into(),
            roles: vec!["federation-reader".into()],
            scopes: vec!["kg:read".into()],
            policy_version: "policy-test".into(),
            delegation: vec![],
            node: None,
            priority: None,
        }
    }

    fn source<'a>(
        secret: &'a str,
        context: &'a RequestContextClaims,
        graph: &'a str,
    ) -> RemoteEngineSource<'a> {
        RemoteEngineSource {
            endpoint: "127.0.0.1:0",
            graph,
            secret,
            context,
            uql: "",
            cypher: "",
            id_field: "id",
        }
    }

    fn request(id: u64, graph: &str, agent_id: &str) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(agent_id.to_string()),
            method: Method::Ping,
        }
    }

    #[test]
    fn live_request_path_produces_a_verified_v2_token() {
        let claims = context();
        let src = source("federation-test-secret", &claims, "g");
        let req = src.signed_request(Method::Ping).unwrap();
        assert!(req.auth_token.starts_with("eg2."));
        assert_eq!(req.agent_id.as_deref(), Some("agent:federation"));
    }

    #[test]
    fn empty_secret_fails_before_dial_instead_of_downgrading() {
        let claims = context();
        let src = source("", &claims, "g");
        assert!(src.signed_request(Method::Ping).is_err());
    }

    #[test]
    fn incomplete_context_fails_before_dial() {
        let mut claims = context();
        claims.policy_version.clear();
        let src = source("federation-test-secret", &claims, "g");
        assert!(src.signed_request(Method::Ping).is_err());
    }

    #[test]
    fn graph_binding_changes_the_token() {
        let claims = context();
        let src = source("federation-test-secret", &claims, "g");
        let token_a = src
            .auth_token_v2(&request(1, "g", &claims.agent_id), 1, "nonce", "idem")
            .unwrap();
        let token_b = src
            .auth_token_v2(
                &request(1, "other-graph", &claims.agent_id),
                1,
                "nonce",
                "idem",
            )
            .unwrap();
        assert_ne!(
            token_a, token_b,
            "signing over a different graph must yield a different token"
        );
    }

    #[test]
    fn tenant_binding_changes_the_token() {
        let claims_a = context();
        let mut claims_b = context();
        claims_b.tenant = "tenant-b".into();
        let req = request(1, "g", &claims_a.agent_id);
        let t1 = source("federation-test-secret", &claims_a, "g")
            .auth_token_v2(&req, 1, "nonce", "idem")
            .unwrap();
        let t2 = source("federation-test-secret", &claims_b, "g")
            .auth_token_v2(&req, 1, "nonce", "idem")
            .unwrap();
        assert_ne!(
            t1, t2,
            "signing over a different tenant must yield a different token"
        );
    }

    #[test]
    fn method_body_binding_changes_the_token() {
        let claims = context();
        let src = source("federation-test-secret", &claims, "g");
        let mut req = request(1, "g", &claims.agent_id);
        req.method = Method::Ping;
        let t1 = src.auth_token_v2(&req, 1, "nonce", "idem").unwrap();
        req.method = Method::Health;
        let t2 = src.auth_token_v2(&req, 1, "nonce", "idem").unwrap();
        assert_ne!(
            t1, t2,
            "signing over a different method must yield a different token"
        );
    }
}
