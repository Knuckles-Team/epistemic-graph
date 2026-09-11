//! The Iceberg REST catalog: what a request addresses, and the operation each
//! address answers.
//!
//! [`super`] owns the listener, the bearer/carrier boundary and the response
//! vocabulary; this module owns routing and the operations themselves. Before
//! the split they were one `handle` of cyclomatic 37 / cognitive 31, in which
//! every route re-derived the caller's visibility and four of them re-emitted
//! the same audit event by hand.

use serde_json::{json, Value};

use super::super::{
    namespace_levels, CreateTableError, LakeManager, LakeVisibility, RenameTableError,
};
use super::{
    carrier_denied_response, err_body, not_found, operation_scope, parse_as_of_lsn,
    parse_pagination, percent_decode, schema_from_create_request, scope_authorized,
    table_ident_namespace, visibility_for,
};
use crate::server::access::CarrierAuthority;
use crate::server::blob::store::ChunkStore;
use crate::server::http1::HttpMessage;

/// Who the caller is for every routed operation: what it may see, and the
/// owner label its audit events carry.
struct RequestScope {
    visibility: LakeVisibility,
    owner: Option<String>,
}

impl RequestScope {
    /// The scope a verified carrier grants, or the anonymous public view.
    fn of(carrier: Option<&CarrierAuthority>) -> Self {
        Self {
            visibility: visibility_for(carrier),
            owner: carrier.map(|c| c.owner_scope().to_string()),
        }
    }

    /// The `owner` field of an audit event: the caller's scope, or `system`.
    fn owner_label(&self) -> String {
        self.owner.clone().unwrap_or_else(|| "system".to_string())
    }
}

/// Record one operation's audit event. Four routes emitted the same five
/// fields around their own subject identity, so the SHAPE lives here and a new
/// route cannot omit a field. The allow/deny verdict stays with the caller
/// because the three derivations genuinely differ — CreateTable and
/// CommitTable succeed with `200 OK`, RenameTable with `204 No Content`, and
/// DropTable reports whether the drop happened at all.
fn record_operation(
    lake: &LakeManager,
    op: &str,
    subject: &[(&str, Value)],
    owner: &str,
    status: &str,
    allowed: bool,
) {
    let mut event = serde_json::Map::new();
    event.insert(
        "ts_ms".to_string(),
        json!(crate::server::lake::lineage::now_ms()),
    );
    event.insert("op".to_string(), json!(op));
    for (key, value) in subject {
        event.insert((*key).to_string(), value.clone());
    }
    event.insert("owner".to_string(), json!(owner));
    event.insert(
        "outcome".to_string(),
        json!(if allowed { "allow" } else { "deny" }),
    );
    event.insert("status".to_string(), json!(status));
    lake.record_audit(Value::Object(event));
}

/// Route + execute one Iceberg REST request. The two guards that precede
/// routing are the browser-origin refusal and the carrier's own scope claim;
/// the scope check runs BEFORE body parsing and existence lookups deliberately
/// (NE-048 P0), so an unauthorized caller learns nothing about whether the
/// target namespace or table even exists.
pub(super) fn handle(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    req: &HttpMessage,
    carrier: Option<&CarrierAuthority>,
) -> (&'static str, String) {
    if !req.header("origin").is_empty() {
        return (
            "403 Forbidden",
            err_body("browser origin denied", "ForbiddenException", 403),
        );
    }
    let (path, _) = req.path_and_query();
    let segs: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(percent_decode)
        .collect();
    let seg_refs: Vec<&str> = segs.iter().map(String::as_str).collect();
    let scope = RequestScope::of(carrier);

    if !scope_authorized(carrier, operation_scope(req.method.as_str(), &seg_refs)) {
        // A denial is not an operation outcome: it carries no status of its own
        // and names the request that was refused, not a namespace or table.
        lake.record_audit(json!({
            "ts_ms": crate::server::lake::lineage::now_ms(),
            "op": "InsufficientScope",
            "method": req.method,
            "path": req.target,
            "owner": scope.owner_label(),
            "outcome": "deny",
        }));
        return carrier_denied_response();
    }

    catalog_route(lake, store, req, &seg_refs, &scope)
}

/// Catalog-level routes: configuration, namespace listing and inspection,
/// table listing and creation, and the cross-namespace rename. Anything else
/// addresses one existing table.
fn catalog_route(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    req: &HttpMessage,
    segs: &[&str],
    scope: &RequestScope,
) -> (&'static str, String) {
    match (req.method.as_str(), segs) {
        ("GET", ["v1", "config"]) => (
            "200 OK",
            json!({ "defaults": {}, "overrides": {} }).to_string(),
        ),
        ("GET", ["v1", "namespaces"]) => {
            let (page_token, page_size) = parse_pagination(&req.target);
            (
                "200 OK",
                lake.list_namespaces_visible(&scope.visibility, page_token.as_deref(), page_size)
                    .to_string(),
            )
        }
        ("GET", ["v1", "namespaces", ns]) => describe_namespace(lake, ns, scope),
        ("GET", ["v1", "namespaces", ns, "tables"]) => {
            let (page_token, page_size) = parse_pagination(&req.target);
            (
                "200 OK",
                lake.list_tables_visible(ns, &scope.visibility, page_token.as_deref(), page_size)
                    .to_string(),
            )
        }
        ("POST", ["v1", "namespaces", ns, "tables"]) => create_table(lake, store, req, ns, scope),
        ("POST", ["v1", "tables", "rename"]) => rename_table(lake, req, scope),
        _ => table_route(lake, store, req, segs, scope),
    }
}

/// Routes addressing ONE table under a namespace.
fn table_route(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    req: &HttpMessage,
    segs: &[&str],
    scope: &RequestScope,
) -> (&'static str, String) {
    match (req.method.as_str(), segs) {
        ("GET", ["v1", "namespaces", ns, "tables", table]) => {
            load_table(lake, req, ns, table, scope)
        }
        ("HEAD", ["v1", "namespaces", ns, "tables", table]) => {
            if lake
                .load_table_visible(ns, table, &scope.visibility)
                .is_some()
            {
                ("200 OK", String::new())
            } else {
                ("404 Not Found", String::new())
            }
        }
        ("POST", ["v1", "namespaces", ns, "tables", table]) => {
            commit_table(lake, store, ns, table, scope)
        }
        ("DELETE", ["v1", "namespaces", ns, "tables", table]) => drop_table(lake, ns, table, scope),
        _ => ("404 Not Found", not_found("route")),
    }
}

/// `GET /v1/namespaces/<ns>` — a namespace the caller may see, else the
/// privacy-safe 404.
fn describe_namespace(
    lake: &LakeManager,
    ns: &str,
    scope: &RequestScope,
) -> (&'static str, String) {
    if !lake.namespace_exists_visible(ns, &scope.visibility) {
        return ("404 Not Found", not_found("namespace"));
    }
    (
        "200 OK",
        json!({ "namespace": namespace_levels(ns), "properties": {} }).to_string(),
    )
}

/// `POST /v1/namespaces/<ns>/tables` — CreateTable.
fn create_table(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    req: &HttpMessage,
    ns: &str,
    scope: &RequestScope,
) -> (&'static str, String) {
    let Ok(body) = serde_json::from_str::<Value>(&req.text()) else {
        return (
            "400 Bad Request",
            err_body(
                "malformed CreateTableRequest body",
                "BadRequestException",
                400,
            ),
        );
    };
    let Some(table) = body.get("name").and_then(Value::as_str).map(str::to_string) else {
        return (
            "400 Bad Request",
            err_body(
                "CreateTableRequest.name is required",
                "BadRequestException",
                400,
            ),
        );
    };
    let schema = match schema_from_create_request(&body) {
        Ok(schema) => schema,
        Err(error) => {
            return (
                "400 Bad Request",
                err_body(&error, "BadRequestException", 400),
            )
        }
    };
    let (status, resp) = match lake.create_table(store, ns, &table, schema, scope.owner.as_deref())
    {
        Ok(created) => ("200 OK", created.to_string()),
        Err(CreateTableError::AlreadyExists) => (
            "409 Conflict",
            err_body(
                &format!("table {ns}.{table} already exists"),
                "AlreadyExistsException",
                409,
            ),
        ),
        Err(CreateTableError::Other(error)) => (
            "400 Bad Request",
            err_body(&error, "BadRequestException", 400),
        ),
    };
    record_operation(
        lake,
        "CreateTable",
        &[("namespace", json!(ns)), ("table", json!(table))],
        &scope.owner_label(),
        status,
        status == "200 OK",
    );
    (status, resp)
}

/// `GET /v1/namespaces/<ns>/tables/<table>` — LoadTable, optionally `?as_of=`.
/// Visibility resolves BEFORE the `as_of` parse or validation, so a hidden
/// table stays the same privacy-safe 404 for malformed, future, hole and
/// overflow LSNs and is never an existence oracle.
fn load_table(
    lake: &LakeManager,
    req: &HttpMessage,
    ns: &str,
    table: &str,
    scope: &RequestScope,
) -> (&'static str, String) {
    let Some(current) = lake.load_table_visible(ns, table, &scope.visibility) else {
        return ("404 Not Found", not_found("table"));
    };
    let as_of = match parse_as_of_lsn(&req.target) {
        Ok(as_of) => as_of,
        Err(()) => {
            return (
                "400 Bad Request",
                err_body(
                    "as_of must be one unsigned decimal LSN",
                    "InvalidAsOfException",
                    400,
                ),
            )
        }
    };
    let Some(lsn) = as_of else {
        return ("200 OK", current.to_string());
    };
    match lake.load_table_as_of(ns, table, lsn, &scope.visibility) {
        Ok(Some(snapshot)) => ("200 OK", snapshot.to_string()),
        // The visibility check above and the manager's scoped check
        // intentionally both fail closed if a concurrent delete or policy
        // change removes the table between the two reads.
        Ok(None) => ("404 Not Found", not_found("table")),
        Err(_) => (
            "400 Bad Request",
            err_body(
                "requested as_of LSN is unavailable",
                "InvalidSnapshotException",
                400,
            ),
        ),
    }
}

/// `POST /v1/namespaces/<ns>/tables/<table>` — CommitTable.
fn commit_table(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    ns: &str,
    table: &str,
    scope: &RequestScope,
) -> (&'static str, String) {
    if lake
        .load_table_visible(ns, table, &scope.visibility)
        .is_none()
    {
        return ("404 Not Found", not_found("table"));
    }
    let (status, resp) = match lake.commit_table(store, ns, table) {
        Ok(committed) => ("200 OK", committed.to_string()),
        Err(error) => (
            "400 Bad Request",
            err_body(&error, "CommitFailedException", 400),
        ),
    };
    record_operation(
        lake,
        "CommitTable",
        &[("namespace", json!(ns)), ("table", json!(table))],
        &scope.owner_label(),
        status,
        status == "200 OK",
    );
    (status, resp)
}

/// `DELETE /v1/namespaces/<ns>/tables/<table>` — DropTable.
fn drop_table(
    lake: &LakeManager,
    ns: &str,
    table: &str,
    scope: &RequestScope,
) -> (&'static str, String) {
    let dropped = lake.drop_table(ns, table, &scope.visibility);
    let (status, resp) = if dropped {
        ("204 No Content", String::new())
    } else {
        ("404 Not Found", not_found("table"))
    };
    record_operation(
        lake,
        "DropTable",
        &[("namespace", json!(ns)), ("table", json!(table))],
        &scope.owner_label(),
        status,
        dropped,
    );
    (status, resp)
}

/// `POST /v1/tables/rename` — RenameTable across namespaces.
fn rename_table(
    lake: &LakeManager,
    req: &HttpMessage,
    scope: &RequestScope,
) -> (&'static str, String) {
    let Ok(body) = serde_json::from_str::<Value>(&req.text()) else {
        return (
            "400 Bad Request",
            err_body(
                "malformed RenameTableRequest body",
                "BadRequestException",
                400,
            ),
        );
    };
    let source = body.get("source").cloned().unwrap_or(Value::Null);
    let destination = body.get("destination").cloned().unwrap_or(Value::Null);
    let (Some(src_ns), Some(src_name)) = (
        table_ident_namespace(&source),
        source.get("name").and_then(Value::as_str),
    ) else {
        return (
            "400 Bad Request",
            err_body("source identifier is required", "BadRequestException", 400),
        );
    };
    let (Some(dst_ns), Some(dst_name)) = (
        table_ident_namespace(&destination),
        destination.get("name").and_then(Value::as_str),
    ) else {
        return (
            "400 Bad Request",
            err_body(
                "destination identifier is required",
                "BadRequestException",
                400,
            ),
        );
    };
    let (status, resp) =
        match lake.rename_table(&src_ns, src_name, &dst_ns, dst_name, &scope.visibility) {
            Ok(()) => ("204 No Content", String::new()),
            Err(RenameTableError::SourceNotFound) => ("404 Not Found", not_found("table")),
            Err(RenameTableError::DestinationExists) => (
                "409 Conflict",
                err_body(
                    &format!("table {dst_ns}.{dst_name} already exists"),
                    "AlreadyExistsException",
                    409,
                ),
            ),
        };
    record_operation(
        lake,
        "RenameTable",
        &[
            ("source", json!(format!("{src_ns}.{src_name}"))),
            ("destination", json!(format!("{dst_ns}.{dst_name}"))),
        ],
        &scope.owner_label(),
        status,
        status == "204 No Content",
    );
    (status, resp)
}
