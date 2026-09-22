use super::*;

use eg_types::contract::{BoundedVec, Digest256};
use eg_types::result_contract::cluster::{
    RegisteredServerCursor, RegisteredServerListPage, RegisteredServerListRequest,
    RegisteredServerView, MAX_REGISTERED_SERVER_PAGE_ENTRIES,
    REGISTERED_SERVER_LIST_SCHEMA_VERSION,
};

const REGISTRY_GRAPH: &str = "__commons__";
const REGISTERED_SERVER_SNAPSHOT_DOMAIN: &[u8] = b"eg/registered-server-snapshot/v1";
const REGISTRY_CURSOR_STALE: &str =
    "REGISTRY_CURSOR_STALE: registered-server snapshot changed; restart from the first page";

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

/// Recursively sort JSON object keys so semantically identical resource maps
/// have one digest representation regardless of their original insertion order.
fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let sorted: std::collections::BTreeMap<_, _> = values.into_iter().collect();
            serde_json::Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

/// Decode one graph row into the closed registry view. Malformed lookalikes are
/// not registry authority: only the exact `srv:<name>` identity, exact
/// `node_type=Server`, complete typed fields, and a live server-time lease pass.
fn registered_server_view(
    node_id: &str,
    properties_msgpack: &[u8],
    observed_at_ms: u64,
) -> Option<RegisteredServerView> {
    let value = eg_types::msgpack::decode_property_value(properties_msgpack).ok()?;
    if value.get("node_type").and_then(serde_json::Value::as_str) != Some("Server") {
        return None;
    }
    let name = value.get("name")?.as_str()?;
    if !valid_register_server_name(name) || node_id != format!("srv:{name}") {
        return None;
    }
    let url = value.get("url")?.as_str()?;
    let resources = value.get("resources")?.clone();
    let ttl_secs = value.get("ttl_secs")?.as_u64()?;
    let registered_at_ms = value.get("registered_at_ms")?.as_u64()?;
    let last_heartbeat_ms = value.get("last_heartbeat_ms")?.as_u64()?;
    let lease_expires_at_ms = value.get("lease_expires_at_ms")?.as_u64()?;
    if url.is_empty()
        || url.len() > MAX_REGISTER_SERVER_URL_BYTES
        || !resources.is_object()
        || !(MIN_REGISTER_SERVER_TTL_SECS..=MAX_REGISTER_SERVER_TTL_SECS).contains(&ttl_secs)
        || lease_expires_at_ms <= observed_at_ms
    {
        return None;
    }
    Some(RegisteredServerView {
        name: name.to_string(),
        url: url.to_string(),
        resources: canonical_json(resources),
        ttl_secs,
        registered_at_ms,
        last_heartbeat_ms,
        lease_expires_at_ms,
    })
}

fn live_registered_servers<F>(
    core: &crate::graph::GraphCore,
    observed_at_ms: u64,
    mut visible: F,
) -> Vec<RegisteredServerView>
where
    F: FnMut(&str, &[u8]) -> bool,
{
    let mut live = core
        .get_nodes_by_label("Server", 0)
        .into_iter()
        .filter(|(node_id, properties)| visible(node_id, properties))
        .filter_map(|(node_id, properties)| {
            registered_server_view(&node_id, &properties, observed_at_ms)
        })
        .collect::<Vec<_>>();
    live.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    live
}

/// Digest the complete live, RLS-visible typed snapshot, not merely the page.
/// Named MessagePack is deterministic because struct field order is fixed and
/// every nested JSON object was recursively canonicalized above.
fn registered_server_snapshot_digest(live: &[RegisteredServerView]) -> Result<Digest256, String> {
    let canonical = rmp_serde::to_vec_named(live)
        .map_err(|error| format!("registered-server snapshot encoding failed: {error}"))?;
    Digest256::framed(
        REGISTERED_SERVER_SNAPSHOT_DOMAIN,
        &[
            &REGISTERED_SERVER_LIST_SCHEMA_VERSION.to_be_bytes(),
            canonical.as_slice(),
        ],
    )
}

fn registered_server_page(
    live: &[RegisteredServerView],
    request: &RegisteredServerListRequest,
    observed_at_ms: u64,
    registry_revision: u64,
) -> Result<RegisteredServerListPage, String> {
    let limit = request
        .limit
        .map(usize::from)
        .unwrap_or(MAX_REGISTERED_SERVER_PAGE_ENTRIES);
    if !(1..=MAX_REGISTERED_SERVER_PAGE_ENTRIES).contains(&limit) {
        return Err(format!(
            "INVALID_ARGUMENT: ListRegisteredServers.limit must be between 1 and {MAX_REGISTERED_SERVER_PAGE_ENTRIES}"
        ));
    }
    let registry_digest = registered_server_snapshot_digest(live)?;
    let start = match request.cursor.as_ref() {
        Some(cursor) => {
            if !valid_register_server_name(&cursor.after_name) {
                return Err(
                    "INVALID_ARGUMENT: ListRegisteredServers cursor has an invalid after_name"
                        .to_string(),
                );
            }
            if cursor.registry_revision != registry_revision
                || cursor.registry_digest != registry_digest
            {
                return Err(REGISTRY_CURSOR_STALE.to_string());
            }
            live.partition_point(|entry| entry.name.as_str() <= cursor.after_name.as_str())
        }
        None => 0,
    };
    let end = start.saturating_add(limit).min(live.len());
    let entries = BoundedVec::new(live[start..end].to_vec())?;
    let next_cursor = (end < live.len()).then(|| RegisteredServerCursor {
        // A non-terminal page always contains at least one row because `limit`
        // is positive and `start < end` whenever `end < live.len()`.
        after_name: live[end - 1].name.clone(),
        registry_revision,
        registry_digest,
    });
    let total_live = u32::try_from(live.len())
        .map_err(|_| "REGISTRY_LIMIT: live server count exceeds u32".to_string())?;
    Ok(RegisteredServerListPage {
        schema_version: REGISTERED_SERVER_LIST_SCHEMA_VERSION,
        entries,
        next_cursor,
        observed_at_ms,
        total_live,
        registry_revision,
        registry_digest,
    })
}

/// Read the live server registry through the same ACL and row-level authority
/// as every graph read. The graph mutex serializes snapshot construction with
/// registration and expiry. This route intentionally reads the local committed
/// `__commons__` image and does not claim a cluster read barrier that does not
/// exist; the single-node/full deployment is the complete authority today.
pub(in crate::server::dispatch) async fn handle_list_registered_servers(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    request: RegisteredServerListRequest,
) -> Response {
    let _registry_guard = crate::server::mutation_batch::lock_graph(REGISTRY_GRAPH).await;
    let (raw_core, read_authority) = {
        let current = timed_read(state).await;
        let Some(entry) = current.registry.get(REGISTRY_GRAPH) else {
            return Response::err(req_id, "registered-server authority is unavailable");
        };
        if let Err(error) = check_graph_access(
            &current.isolation,
            Some(verified_context.agent_id()),
            REGISTRY_GRAPH,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        ) {
            return Response::err(req_id, error);
        }
        let read_authority =
            match GraphReadAuthority::from_verified(verified_context, &current.isolation) {
                Ok(authority) => authority,
                Err(error) => return Response::err(req_id, error),
            };
        (entry.core.clone(), read_authority)
    };

    let registry_revision = raw_core.version();
    let observed_at_ms = authoritative_now_ms();
    let live = live_registered_servers(&raw_core, observed_at_ms, |node_id, properties| {
        read_authority.can_see_node(properties, raw_core.is_schema_node(node_id))
    });
    match registered_server_page(&live, &request, observed_at_ms, registry_revision) {
        Ok(page) => match ResultPayload::of::<
            eg_types::result_contract::cluster::ListRegisteredServers,
        >(page)
        {
            Ok(payload) => Response::ok(req_id, payload),
            Err(error) => Response::err(req_id, error),
        },
        Err(error) => Response::err(req_id, error),
    }
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

#[cfg(test)]
mod list_registered_servers_tests {
    use super::*;

    fn row(name: &str, lease_expires_at_ms: u64) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({
            "node_type": "Server",
            "name": name,
            "url": format!("http://{name}"),
            "resources": {"z": 1, "a": {"y": 2, "b": 3}},
            "ttl_secs": 60,
            "registered_at_ms": 1,
            "last_heartbeat_ms": 2,
            "lease_expires_at_ms": lease_expires_at_ms,
        }))
        .unwrap()
    }

    #[cfg(feature = "security")]
    fn private_row(name: &str, owner: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({
            "node_type": "Server",
            "name": name,
            "url": format!("http://{name}"),
            "resources": {},
            "ttl_secs": 60,
            "registered_at_ms": 1,
            "last_heartbeat_ms": 2,
            "lease_expires_at_ms": 100_000,
            "_owner": owner,
            "_visibility": "private",
        }))
        .unwrap()
    }

    fn live(names: &[&str]) -> Vec<RegisteredServerView> {
        let core = crate::graph::GraphCore::new();
        for name in names {
            core.add_node(format!("srv:{name}"), row(name, 100_000));
        }
        live_registered_servers(&core, 50_000, |_, _| true)
    }

    #[test]
    fn typed_live_snapshot_is_name_sorted_and_filters_invalid_or_expired_rows() {
        let core = crate::graph::GraphCore::new();
        core.add_node("srv:zeta".to_string(), row("zeta", 100_000));
        core.add_node("srv:alpha".to_string(), row("alpha", 100_000));
        core.add_node("srv:expired".to_string(), row("expired", 50_000));
        core.add_node("wrong-id".to_string(), row("lookalike", 100_000));
        core.add_node(
            "srv:not-server".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "Other",
                "name": "not-server",
            }))
            .unwrap(),
        );

        let snapshot = live_registered_servers(&core, 50_000, |_, _| true);
        assert_eq!(
            snapshot
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert_eq!(
            snapshot[0].resources,
            serde_json::json!({"a": {"b": 3, "y": 2}, "z": 1})
        );
    }

    #[test]
    fn cursor_is_exclusive_and_fenced_by_revision_and_full_snapshot_digest() {
        let snapshot = live(&["charlie", "alpha", "bravo"]);
        let first = registered_server_page(
            &snapshot,
            &RegisteredServerListRequest {
                limit: Some(1),
                cursor: None,
            },
            50_000,
            7,
        )
        .unwrap();
        assert_eq!(first.entries.as_slice()[0].name, "alpha");
        let cursor = first.next_cursor.expect("the first page continues");

        let second = registered_server_page(
            &snapshot,
            &RegisteredServerListRequest {
                limit: Some(1),
                cursor: Some(cursor.clone()),
            },
            50_001,
            7,
        )
        .unwrap();
        assert_eq!(second.entries.as_slice()[0].name, "bravo");

        let wrong_revision = registered_server_page(
            &snapshot,
            &RegisteredServerListRequest {
                limit: Some(1),
                cursor: Some(cursor.clone()),
            },
            50_001,
            8,
        )
        .unwrap_err();
        assert_eq!(wrong_revision, REGISTRY_CURSOR_STALE);

        let changed = live(&["charlie", "alpha", "bravo", "delta"]);
        let wrong_digest = registered_server_page(
            &changed,
            &RegisteredServerListRequest {
                limit: Some(1),
                cursor: Some(cursor),
            },
            50_001,
            7,
        )
        .unwrap_err();
        assert_eq!(wrong_digest, REGISTRY_CURSOR_STALE);
    }

    #[test]
    fn digest_is_independent_of_json_object_insertion_order() {
        let mut left = live(&["alpha"]);
        let mut right = left.clone();
        left[0].resources = canonical_json(serde_json::json!({"z": 1, "a": 2}));
        right[0].resources = canonical_json(serde_json::json!({"a": 2, "z": 1}));
        assert_eq!(
            registered_server_snapshot_digest(&left).unwrap(),
            registered_server_snapshot_digest(&right).unwrap()
        );
    }

    #[cfg(feature = "security")]
    #[test]
    fn hidden_server_row_is_absent_from_count_and_snapshot_digest() {
        use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};

        let core = crate::graph::GraphCore::new();
        core.add_node(
            "srv:alice-server".to_string(),
            private_row("alice-server", "alice"),
        );
        core.add_node(
            "srv:bob-server".to_string(),
            private_row("bob-server", "bob"),
        );
        let mut isolation = IsolationLayer::new();
        for agent_id in ["alice", "bob"] {
            isolation.register_agent(AgentIdentity {
                agent_id: agent_id.to_string(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles: Vec::new(),
            });
        }
        let context = VerifiedRequestContext::verified_for_test("alice");
        let authority = GraphReadAuthority::from_verified(&context, &isolation).unwrap();
        let visible = live_registered_servers(&core, 50_000, |node_id, properties| {
            authority.can_see_node(properties, core.is_schema_node(node_id))
        });
        let unfiltered = live_registered_servers(&core, 50_000, |_, _| true);
        assert_eq!(
            visible
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            ["alice-server"]
        );
        let page = registered_server_page(
            &visible,
            &RegisteredServerListRequest {
                limit: None,
                cursor: None,
            },
            50_000,
            core.version(),
        )
        .unwrap();
        assert_eq!(page.total_live, 1);
        assert_ne!(
            page.registry_digest,
            registered_server_snapshot_digest(&unfiltered).unwrap(),
            "a hidden row must not influence the caller-visible registry digest"
        );
    }

    #[test]
    fn page_limit_is_positive_and_bounded() {
        let snapshot = live(&["alpha"]);
        for limit in [Some(0), Some(257)] {
            let error = registered_server_page(
                &snapshot,
                &RegisteredServerListRequest {
                    limit,
                    cursor: None,
                },
                50_000,
                1,
            )
            .unwrap_err();
            assert!(error.starts_with("INVALID_ARGUMENT:"));
        }
    }
}
