//! Published engine error vocabulary. The typed enum families are the source
//! of the codes; this module supplies transport metadata and method routing.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::connector_pack::result::PackWriteErrorCode;
use eg_types::contract::{EngineErrorCode, ServerErrorCode};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::DecisionErrorCode;
use eg_types::graph_schema::GraphSchemaErrorCode;
use eg_types::solve::SolveErrorCode;

use crate::{MethodDescriptor, Stability};

fn typed_codes<T: Copy>(variants: &[T], code: impl Fn(T) -> &'static str) -> Vec<&'static str> {
    variants.iter().copied().map(code).collect()
}

fn families() -> [(&'static str, Vec<&'static str>); 7] {
    [
        (
            "engine",
            typed_codes(EngineErrorCode::ALL, EngineErrorCode::as_str),
        ),
        (
            "decision",
            typed_codes(DecisionErrorCode::ALL, DecisionErrorCode::as_str),
        ),
        (
            "decision",
            typed_codes(StatisticalErrorCode::ALL, StatisticalErrorCode::as_str),
        ),
        (
            "schema",
            typed_codes(GraphSchemaErrorCode::ALL, GraphSchemaErrorCode::as_str),
        ),
        (
            "connector",
            typed_codes(PackWriteErrorCode::ALL, PackWriteErrorCode::as_str),
        ),
        (
            "solve",
            typed_codes(SolveErrorCode::ALL, SolveErrorCode::as_str),
        ),
        (
            "server",
            typed_codes(ServerErrorCode::ALL, ServerErrorCode::as_str),
        ),
    ]
}

const CODE_CLASSES: &[(&str, &str)] = &[
    ("AUTH_TENANT_MISMATCH", "auth"),
    ("AUTH_AUDIENCE_MISMATCH", "auth"),
    ("AUTH_POLICY_VERSION_MISMATCH", "auth"),
    ("NODE_MISMATCH", "auth"),
    ("ACCESS_DENIED", "policy"),
    ("POLICY_NATIVE_AUTHORITY_REQUIRED", "policy"),
    ("CAPACITY_DENIED", "capacity"),
    ("ENGINE_RESOURCE_EXHAUSTED", "capacity"),
    ("UQL_BUDGET_EXCEEDED", "capacity"),
    ("REDIRECTED", "availability"),
    ("ENGINE_UNAVAILABLE", "availability"),
    ("ENGINE_DEADLINE_EXCEEDED", "availability"),
    ("CANCELLED", "availability"),
    ("TELEMETRY_UNAVAILABLE", "availability"),
    ("CAPACITY_UNAVAILABLE", "availability"),
    ("DECISIONS", "availability"),
    ("CONFLICT", "conflict"),
    ("IDEMPOTENCY_CONFLICT", "conflict"),
    ("READ_ONLY", "conflict"),
    ("AUTHENTICATION_REQUIRED", "auth"),
    ("BUSY", "capacity"),
    ("INTERNAL", "internal"),
    ("ELEVATION_ACTOR_UNSTAMPED", "internal"),
    ("SCHEMA_INCONSISTENT", "schema"),
];

const FAMILY_CLASSES: &[(&str, &str)] = &[
    ("engine", "validation"),
    ("decision", "decision"),
    ("schema", "schema"),
    ("connector", "connector"),
    ("solve", "solve"),
    ("server", "server"),
];

fn class(code: &str, family: &str) -> &'static str {
    CODE_CLASSES
        .iter()
        .find_map(|(token, class)| (*token == code).then_some(*class))
        .or_else(|| {
            FAMILY_CLASSES
                .iter()
                .find_map(|(name, class)| (*name == family).then_some(*class))
        })
        .expect("closed error family")
}

fn retryable(code: &str) -> bool {
    matches!(
        code,
        "REDIRECTED"
            | "ENGINE_UNAVAILABLE"
            | "ENGINE_DEADLINE_EXCEEDED"
            | "ENGINE_RESOURCE_EXHAUSTED"
            | "CAPACITY_DENIED"
            | "READ_ONLY"
            | "BUSY"
    )
}

const HTTP_HINTS: &[(&str, u16)] = &[
    ("AUTH_TENANT_MISMATCH", 403),
    ("AUTH_AUDIENCE_MISMATCH", 403),
    ("AUTH_POLICY_VERSION_MISMATCH", 403),
    ("NODE_MISMATCH", 403),
    ("ACCESS_DENIED", 403),
    ("POLICY_NATIVE_AUTHORITY_REQUIRED", 403),
    ("REDIRECTED", 307),
    ("ENGINE_UNAVAILABLE", 503),
    ("READ_ONLY", 503),
    ("ENGINE_DEADLINE_EXCEEDED", 504),
    ("CAPACITY_DENIED", 429),
    ("ENGINE_RESOURCE_EXHAUSTED", 429),
    ("UQL_BUDGET_EXCEEDED", 429),
    ("CONFLICT", 409),
    ("IDEMPOTENCY_CONFLICT", 409),
    ("AUTHENTICATION_REQUIRED", 401),
    ("BUSY", 429),
    ("TIMEOUT", 504),
    ("INTERNAL", 500),
    ("CANCELLED", 408),
    ("TELEMETRY_UNAVAILABLE", 503),
    ("CAPACITY_UNAVAILABLE", 503),
    ("DECISIONS", 503),
    ("ELEVATION_ACTOR_UNSTAMPED", 500),
    ("SCHEMA_INCONSISTENT", 409),
    ("GRAPH_NOT_FOUND", 404),
    ("TELEMETRY_WINDOW_TOO_LARGE", 413),
];

fn http_status_hint(code: &str) -> u16 {
    HTTP_HINTS
        .iter()
        .find_map(|(token, hint)| (*token == code).then_some(*hint))
        .unwrap_or(400)
}

pub(super) fn catalog_json() -> Vec<u8> {
    let mut rows = BTreeMap::new();
    for (family, codes) in families() {
        for code in codes {
            let row = serde_json::json!({
                "code": code,
                "class": class(code, family),
                "retryable": retryable(code),
                "http_status_hint": http_status_hint(code),
            });
            if let Some(previous) = rows.insert(code, row.clone()) {
                assert_eq!(previous, row, "conflicting metadata for {code}");
            }
        }
    }
    super::pretty(&serde_json::json!({
        "contract_version": 1,
        "generator": "eg-capabilities/gen_contract",
        "errors": rows.into_values().collect::<Vec<_>>(),
    }))
}

/// Shared refusals apply before the request reaches its method handler.
const SHARED_ENGINE_ERRORS: &[&str] = &[
    "INVALID_ARGUMENT",
    "ACCESS_DENIED",
    "AUTH_TENANT_MISMATCH",
    "AUTH_AUDIENCE_MISMATCH",
    "AUTH_POLICY_VERSION_MISMATCH",
    "NODE_MISMATCH",
    "CAPACITY_DENIED",
    "ENGINE_UNAVAILABLE",
    "ENGINE_RESOURCE_EXHAUSTED",
    "ENGINE_DEADLINE_EXCEEDED",
];
const SHARED_SERVER_ERRORS: &[&str] = &[
    "AUTHENTICATION_REQUIRED",
    "BUSY",
    "CANCELLED",
    "INTERNAL",
    "TIMEOUT",
];

/// A method-specific family wins over the wider domain family.
const METHOD_PREFIXES: &[(&str, &[&str])] = &[
    (
        "Source",
        &[
            "SOURCE_",
            "CONNECTOR_SCHEMA_",
            "STALE_MAPPING_",
            "UNKNOWN_MAPPING_",
            "UNKNOWN_RELATIONSHIP_",
        ],
    ),
    (
        "ConnectorPack",
        &[
            "PACK_",
            "CONNECTOR_PACK_",
            "CONNECTOR_RELATIONSHIP_",
            "ARCHIVE_",
            "MALFORMED_",
            "IMPORTER_",
            "INVALID_IMPORTER",
            "INVALID_RETIREMENT",
            "INVALID_FACTS",
            "UNKNOWN_PACK_ENTRY",
            "CORRUPT_CONNECTOR_",
            "COMPONENT_BODY_",
        ],
    ),
    (
        "GraphSchema",
        &[
            "ATTACH_PACK_",
            "PACK_PROJECTION_",
            "OWL_",
            "REASONING_",
            "STALE_RECOMPUTE_",
        ],
    ),
    (
        "Decision",
        &["DECISION_", "COMPONENT_", "CORRUPT_DECISION_"],
    ),
    ("Decide", &["DECISION_", "COMPONENT_", "CORRUPT_DECISION_"]),
];
const DOMAIN_PREFIXES: &[(&str, &[&str])] = &[
    ("cluster", &["CLUSTER_", "FLEET_", "REGISTRY_", "RAFT_"]),
    ("query", &["AST_", "SQL_OWNER_", "RESULT_TOO_LARGE"]),
    (
        "transactions",
        &["CHANGE_BATCH_", "REPLAY_NONCE_", "STALE_GRAPH_"],
    ),
];
const EXACT_METHOD_ERRORS: &[(&str, &[&str])] = &[
    ("ApplyChangeEnvelopes", &["ABORTED_ATOMIC_GRAPH_BATCH"]),
    (
        "RegisterForeignSource",
        &["FOREIGN_SOURCE_POLICY_UNBOOTSTRAPPED"],
    ),
    (
        "MutationOutbox",
        &[
            "CORRUPT_OUTBOX",
            "OUTBOX_OWNER_UNAVAILABLE",
            "OUTBOX_OWNER_UNKNOWN",
            "OUTBOX_SCOPE_MISMATCH",
        ],
    ),
    ("AgentLibrary", &["STALE_AGENT_LIBRARY_REVISION"]),
    ("WriteBack", &["CORRUPT_WRITE_BACK"]),
    ("TxnPlanWriteback", &["CORRUPT_WRITE_BACK"]),
    (
        "TelemetryDerive",
        &[
            "TELEMETRY_UNAVAILABLE",
            "TELEMETRY_WINDOW_TOO_LARGE",
            "SCHEMA_INCONSISTENT",
            "GRAPH_NOT_FOUND",
        ],
    ),
    ("RbacElevation", &["ELEVATION_ACTOR_UNSTAMPED"]),
    (
        "Decide",
        &["DECISIONS", "UNSUPPORTED_COALITION", "CAPACITY_UNAVAILABLE"],
    ),
];

fn add_engine_errors(d: &MethodDescriptor, codes: &mut BTreeSet<&'static str>) {
    for code in EngineErrorCode::ALL {
        let token = code.as_str();
        if SHARED_ENGINE_ERRORS.contains(&token)
            || (d.domain == "query" && token.starts_with("UQL_"))
        {
            codes.insert(token);
        }
    }
}

fn server_prefixes(d: &MethodDescriptor) -> &'static [&'static str] {
    METHOD_PREFIXES
        .iter()
        .find_map(|(method, prefixes)| d.id.as_str().starts_with(method).then_some(*prefixes))
        .or_else(|| {
            DOMAIN_PREFIXES
                .iter()
                .find_map(|(domain, prefixes)| (d.domain == *domain).then_some(*prefixes))
        })
        .unwrap_or(&[])
}

fn add_server_errors(d: &MethodDescriptor, codes: &mut BTreeSet<&'static str>) {
    let prefixes = server_prefixes(d);
    for code in ServerErrorCode::ALL {
        let token = code.as_str();
        if SHARED_SERVER_ERRORS.contains(&token)
            || prefixes.iter().any(|prefix| token.starts_with(prefix))
        {
            codes.insert(token);
        }
    }
}

fn add_owner_errors(d: &MethodDescriptor, codes: &mut BTreeSet<&'static str>) {
    if d.policy.is_durable() {
        codes.insert("CORRUPT_MUTATION_LEDGER");
    }
    if d.stability == Stability::Internal {
        codes.insert("METHOD_NOT_YET_SERVED");
    }
    if d.domain == "security" {
        codes.insert("POLICY_NATIVE_AUTHORITY_REQUIRED");
    }
    if d.id.as_str().starts_with("Blob") {
        codes.insert("BODY_DIGEST_MISMATCH");
    }
    for (method, extras) in EXACT_METHOD_ERRORS {
        if d.id.as_str() == *method {
            codes.extend(extras.iter().copied());
        }
    }
}

fn add_typed_handler_errors(d: &MethodDescriptor, codes: &mut BTreeSet<&'static str>) {
    let id = d.id.as_str();
    if id.starts_with("Decision") || id.starts_with("Decide") {
        codes.extend(typed_codes(
            DecisionErrorCode::ALL,
            DecisionErrorCode::as_str,
        ));
        codes.extend(typed_codes(
            StatisticalErrorCode::ALL,
            StatisticalErrorCode::as_str,
        ));
    }
    if id.starts_with("GraphSchema") {
        codes.extend(typed_codes(
            GraphSchemaErrorCode::ALL,
            GraphSchemaErrorCode::as_str,
        ));
    }
    if id.starts_with("ConnectorPack") {
        codes.extend(typed_codes(
            PackWriteErrorCode::ALL,
            PackWriteErrorCode::as_str,
        ));
    }
    if id == "Solve" {
        codes.extend(typed_codes(SolveErrorCode::ALL, SolveErrorCode::as_str));
    }
}

/// Include shared boundary refusals and the typed codes that a method's
/// handler family can return. A sorted set keeps the generated API stable.
pub(super) fn method_error_set(d: &MethodDescriptor) -> Vec<&'static str> {
    let mut codes: BTreeSet<_> = d.error_set.iter().copied().collect();
    add_engine_errors(d, &mut codes);
    add_server_errors(d, &mut codes);
    add_owner_errors(d, &mut codes);
    add_typed_handler_errors(d, &mut codes);
    codes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn server_sources(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("server source directory") {
            let path = entry.expect("server source entry").path();
            let name = path.file_name().unwrap().to_string_lossy();
            if name.contains("test") {
                continue;
            }
            if path.is_dir() {
                server_sources(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    fn literal_refusals(line: &str) -> impl Iterator<Item = &str> {
        line.split('"').skip(1).step_by(2).filter_map(|quoted| {
            let (code, _) = quoted.split_once(": ")?;
            (code.len() >= 4
                && code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
            .then_some(code)
        })
    }

    #[test]
    fn every_method_error_is_in_the_published_catalog() {
        let catalog: serde_json::Value = serde_json::from_slice(&catalog_json()).unwrap();
        let codes: BTreeSet<_> = catalog["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["code"].as_str().unwrap())
            .collect();
        let mut routed = BTreeSet::new();
        for method in crate::method_descriptors() {
            for code in method_error_set(&method) {
                assert!(codes.contains(code), "{}: {code}", method.id.as_str());
                routed.insert(code);
            }
        }
        assert_eq!(routed, codes, "every published error needs a method route");
        assert!(codes.contains("AUTH_TENANT_MISMATCH"));
        assert!(codes.contains("AUTH_AUDIENCE_MISMATCH"));
        assert!(codes.contains("AUTH_POLICY_VERSION_MISMATCH"));
    }

    #[test]
    fn literal_server_refusals_are_in_the_published_catalog() {
        let catalog: serde_json::Value = serde_json::from_slice(&catalog_json()).unwrap();
        let codes: BTreeSet<_> = catalog["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["code"].as_str().unwrap())
            .collect();
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src/server");
        let mut files = Vec::new();
        server_sources(&root, &mut files);
        assert!(!files.is_empty(), "server source census is empty");
        for file in files {
            let source = std::fs::read_to_string(&file).expect("read server source");
            for (line_number, line) in source.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                for code in literal_refusals(line) {
                    assert!(
                        codes.contains(code),
                        "{}:{}: {code}",
                        file.display(),
                        line_number + 1
                    );
                }
            }
        }
    }

    #[test]
    fn wire_callable_flags_match_published_and_internal_rows() {
        let catalog = super::super::Catalog::collect();
        let document: serde_json::Value =
            serde_json::from_slice(&super::super::methods_json(&catalog)).unwrap();
        let methods = document["methods"].as_array().unwrap();
        let row = |id| {
            methods
                .iter()
                .find(|method| method["id"].as_str() == Some(id))
                .unwrap()
        };
        assert_eq!(row("Health")["is_wire_callable"], true);
        assert_eq!(row("RegisterUdf")["is_wire_callable"], false);
    }
}
