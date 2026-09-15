use super::*;

/// Civil (proleptic Gregorian) `(year, month, day)` from a days-since-1970-01-01
/// count -- Howard Hinnant's `civil_from_days`, the SAME proven, dependency-free
/// algorithm `eg-rdf`'s `sparql::civil_from_days` already uses for XSD `dateTime`
/// formatting (deliberately re-derived here rather than imported: the facade
/// does not otherwise depend on `eg-rdf` internals, and this is a small, fully
/// self-contained pure function).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Render `unix_secs` as `%Y-%m-%dT%H:%M:%SZ` -- the SAME format au's
/// `engine_ingestion.ingest_mcp_server`/`engine_mcp_discovery.check_server_freshness`
/// already read/write for a `:Server` node's `timestamp` field, so an
/// engine-registered server stays readable by the existing au freshness check.
fn format_iso8601_seconds(unix_secs: u64) -> String {
    let secs = unix_secs as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// `RegisterServer.name` validity -- mirrors au's `_SERVER_NAME` regex
/// (`^[A-Za-z0-9_.-]{1,128}$`) byte-for-byte so the same name is valid on both
/// the au config-sync path and this engine-native push-registration path.
fn valid_register_server_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_REGISTER_SERVER_NAME_BYTES
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// `Method::RegisterServer`'s handler (CONCEPT:EG-KG.sharding.server-registry, W2.5):
/// validate, compute the server-authoritative lease fields, build the `:Server`
/// property blob (preserving `registered_at_ms` across a renewal -- a heartbeat
/// is just a repeat call with the same `name`), and delegate to the ordinary
/// graph gateway via a translated `Method::AddNode` against `__commons__` --
/// see the `Method::RegisterServer` doc comment in `protocol.rs` and
/// `server::mutation::NON_GATEWAY_COORDINATED`'s `RegisterServer` entry. Never
/// trusts a caller-supplied timestamp: every lease field is derived from
/// [`authoritative_now_ms`].
// Mirrors `build_envelope_v2_bytes` (protocol.rs): a wire-marshaling function
// over genuinely-required distinct fields, with no natural grouping that
// wouldn't just be a single-use wrapper struct.
#[allow(clippy::too_many_arguments)]
pub(in crate::server::dispatch) async fn handle_register_server(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    name: String,
    url: String,
    resources_json: String,
    ttl_secs: u64,
) -> Response {
    if !valid_register_server_name(&name) {
        return Response::err(
            req_id,
            "RegisterServer.name must be a bounded logical name (^[A-Za-z0-9_.-]{1,128}$)",
        );
    }
    if url.is_empty() || url.len() > MAX_REGISTER_SERVER_URL_BYTES {
        return Response::err(req_id, "RegisterServer.url exceeds resource limits");
    }
    if resources_json.len() > MAX_REGISTER_SERVER_RESOURCES_BYTES {
        return Response::err(
            req_id,
            "RegisterServer.resources_json exceeds resource limits",
        );
    }
    let resources = if resources_json.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_str::<serde_json::Value>(&resources_json) {
            Ok(value @ serde_json::Value::Object(_)) => value,
            _ => {
                return Response::err(
                    req_id,
                    "RegisterServer.resources_json must be a JSON object",
                )
            }
        }
    };
    if !(MIN_REGISTER_SERVER_TTL_SECS..=MAX_REGISTER_SERVER_TTL_SECS).contains(&ttl_secs) {
        return Response::err(
            req_id,
            format!(
                "RegisterServer.ttl_secs must be between {MIN_REGISTER_SERVER_TTL_SECS} and \
                 {MAX_REGISTER_SERVER_TTL_SECS}"
            ),
        );
    }

    let node_id = format!("srv:{name}");
    let now_ms = authoritative_now_ms();
    let lease_expires_at_ms = now_ms.saturating_add(ttl_secs.saturating_mul(1_000));

    // Preserve `registered_at_ms` across a renewal by peeking at any existing row
    // -- read-only, off the always-resident `__commons__` core, never a
    // durability-relevant read (a race with a concurrent first-registration at
    // worst repeats `now_ms`, never loses data).
    let registered_at_ms = {
        let s = timed_read(state).await;
        s.registry
            .get("__commons__")
            .and_then(|entry| entry.core.get_node_properties(&node_id))
            .and_then(|blob| eg_types::msgpack::decode_property_value(&blob).ok())
            .and_then(|value| value.get("registered_at_ms").and_then(|v| v.as_u64()))
            .unwrap_or(now_ms)
    };

    let properties = serde_json::json!({
        "node_type": "Server",
        "name": name,
        "url": url,
        "resources": resources,
        "timestamp": format_iso8601_seconds(now_ms / 1_000),
        "ttl_secs": ttl_secs,
        "registered_at_ms": registered_at_ms,
        "last_heartbeat_ms": now_ms,
        "lease_expires_at_ms": lease_expires_at_ms,
    });
    let properties_msgpack = match rmp_serde::to_vec_named(&properties) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Response::err(
                req_id,
                format!("RegisterServer payload encode failed: {error}"),
            )
        }
    };

    register_server_response(
        dispatch_graph_op(
            state,
            "__commons__",
            req_id,
            caller,
            verified_context,
            Method::AddNode {
                node_id,
                properties_msgpack,
            },
        )
        .await,
    )
}

/// `RegisterServer` answers with the acknowledgement of the `AddNode` server-row
/// write it performs, declared under its own marker.
fn register_server_response(response: Response) -> Response {
    match response {
        Response {
            id,
            error: Some(error),
            ..
        } => Response::err(id, error),
        Response {
            id,
            result: Some(ResultPayload::String(acknowledgement)),
            ..
        } => Response::ok(
            id,
            ResultPayload::scalar::<eg_types::result_contract::cluster::RegisterServer>(
                acknowledgement,
            ),
        ),
        Response { id, .. } => Response::err(
            id,
            "RegisterServer: the server-row write answered no acknowledgement",
        ),
    }
}
