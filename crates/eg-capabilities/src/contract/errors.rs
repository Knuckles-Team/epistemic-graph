//! Published engine error vocabulary. The typed enum families are the source
//! of the codes; this module supplies transport metadata and method routing.

use std::collections::BTreeMap;

use eg_types::connector_pack::result::PackWriteErrorCode;
use eg_types::contract::{EngineErrorCode, ServerErrorCode};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::DecisionErrorCode;
use eg_types::graph_schema::GraphSchemaErrorCode;
use eg_types::solve::SolveErrorCode;

pub(super) use crate::error_routing::method_error_set;
use crate::error_routing::typed_codes;

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
    ("OPERATION_REDIRECTED", "availability"),
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
            | "OPERATION_REDIRECTED"
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
    ("OPERATION_REDIRECTED", 307),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
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
