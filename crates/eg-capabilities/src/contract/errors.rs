//! Published engine error vocabulary. The typed enum families are the source
//! of the codes; this module supplies transport metadata and method routing.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::connector_pack::result::PackWriteErrorCode;
use eg_types::contract::EngineErrorCode;
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::DecisionErrorCode;
use eg_types::graph_schema::GraphSchemaErrorCode;
use eg_types::solve::SolveErrorCode;

use crate::MethodDescriptor;

fn typed_codes<T: Copy>(variants: &[T], code: impl Fn(T) -> &'static str) -> Vec<&'static str> {
    variants.iter().copied().map(code).collect()
}

fn families() -> [(&'static str, Vec<&'static str>); 6] {
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
    ]
}

fn class(code: &str, family: &str) -> &'static str {
    match code {
        "AUTH_TENANT_MISMATCH"
        | "AUTH_AUDIENCE_MISMATCH"
        | "AUTH_POLICY_VERSION_MISMATCH"
        | "NODE_MISMATCH" => "auth",
        "ACCESS_DENIED" | "POLICY_NATIVE_AUTHORITY_REQUIRED" => "policy",
        "CAPACITY_DENIED" | "ENGINE_RESOURCE_EXHAUSTED" | "UQL_BUDGET_EXCEEDED" => "capacity",
        "REDIRECTED" | "ENGINE_UNAVAILABLE" | "ENGINE_DEADLINE_EXCEEDED" => "availability",
        "CONFLICT" | "IDEMPOTENCY_CONFLICT" | "READ_ONLY" => "conflict",
        _ => match family {
            "engine" => "validation",
            "decision" => "decision",
            "schema" => "schema",
            "connector" => "connector",
            "solve" => "solve",
            _ => unreachable!("closed family"),
        },
    }
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
    )
}

fn http_status_hint(code: &str) -> u16 {
    match code {
        "AUTH_TENANT_MISMATCH"
        | "AUTH_AUDIENCE_MISMATCH"
        | "AUTH_POLICY_VERSION_MISMATCH"
        | "NODE_MISMATCH"
        | "ACCESS_DENIED"
        | "POLICY_NATIVE_AUTHORITY_REQUIRED" => 403,
        "REDIRECTED" => 307,
        "ENGINE_UNAVAILABLE" | "READ_ONLY" => 503,
        "ENGINE_DEADLINE_EXCEEDED" => 504,
        "CAPACITY_DENIED" | "ENGINE_RESOURCE_EXHAUSTED" | "UQL_BUDGET_EXCEEDED" => 429,
        "CONFLICT" | "IDEMPOTENCY_CONFLICT" => 409,
        _ => 400,
    }
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

/// Include shared boundary refusals and the typed codes that a method's
/// handler family can return. A sorted set keeps the generated API stable.
pub(super) fn method_error_set(d: &MethodDescriptor) -> Vec<&'static str> {
    let mut codes: BTreeSet<_> = d.error_set.iter().copied().collect();
    for code in EngineErrorCode::ALL {
        let token = code.as_str();
        if !token.starts_with("UQL_") || d.domain == "query" {
            codes.insert(token);
        }
    }
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
    codes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_method_error_is_in_the_published_catalog() {
        let catalog: serde_json::Value = serde_json::from_slice(&catalog_json()).unwrap();
        let codes: BTreeSet<_> = catalog["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["code"].as_str().unwrap())
            .collect();
        for method in crate::method_descriptors() {
            for code in method_error_set(&method) {
                assert!(codes.contains(code), "{}: {code}", method.id.as_str());
            }
        }
        assert!(codes.contains("AUTH_TENANT_MISMATCH"));
        assert!(codes.contains("AUTH_AUDIENCE_MISMATCH"));
        assert!(codes.contains("AUTH_POLICY_VERSION_MISMATCH"));
    }
}
