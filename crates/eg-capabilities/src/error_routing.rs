//! One runtime and generated-contract error-set derivation per method.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use eg_types::connector_pack::result::PackWriteErrorCode;
use eg_types::contract::{EngineErrorCode, ServerErrorCode, UqlCode};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::DecisionErrorCode;
use eg_types::graph_schema::GraphSchemaErrorCode;
use eg_types::solve::SolveErrorCode;

use crate::MethodDescriptor;

pub(crate) fn typed_codes<T: Copy>(
    variants: &[T],
    code: impl Fn(T) -> &'static str,
) -> Vec<&'static str> {
    variants.iter().copied().map(code).collect()
}

/// Shared refusals apply before the request reaches its method handler.
const SHARED_ENGINE_ERRORS: &[&str] = &[
    // The request boundary refuses any request naming an engine-internal method.
    "ENGINE_INTERNAL_METHOD",
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
    "OPERATION_REDIRECTED",
];
const SHARED_SERVER_ERRORS: &[&str] = &[
    "AUTHENTICATION_REQUIRED",
    "BUSY",
    "CANCELLED",
    "INTERNAL",
    "METHOD_NOT_YET_SERVED",
    "TIMEOUT",
    "STALE_ROUTE",
    "STALE_OUTBOX_LEASE",
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
    ("RbacElevation", &["ELEVATION_"]),
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
    // `ChangeEnvelope::validate` refuses carried engine-internal/native operations.
    ("ApplyChangeEnvelope", &["CARRIER_INNER_METHOD_REFUSED"]),
    ("GetNodes", &["RESULT_TOO_LARGE"]),
    ("GetEdges", &["RESULT_TOO_LARGE"]),
    (
        "CreateGraph",
        &["REPLAY_NONCE_CONSUMED", "RESERVED_GRAPH_NAME"],
    ),
    ("AddNode", &["REPLAY_NONCE_CONSUMED"]),
    ("CypherQuery", &["REPLAY_NONCE_CONSUMED"]),
    ("GraphQl", &["REPLAY_NONCE_CONSUMED"]),
    ("SubmitWorkItem", &["REPLAY_NONCE_CONSUMED"]),
    ("ClaimWorkItem", &["REPLAY_NONCE_CONSUMED"]),
    ("TsListSeries", &["REPLAY_NONCE_CONSUMED"]),
    // delegation/validation.rs refuses a retired Agent Library head or entry.
    (
        "KgDelegate",
        &[
            "CLUSTER_CONFIGURATION_INVALID",
            "REPLAY_NONCE_CONSUMED",
            "STALE_AGENT_LIBRARY_REVISION",
        ],
    ),
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
    if matches!(id, "Uql" | "NlQuery" | "KnowledgeStream") {
        codes.extend(typed_codes(UqlCode::ALL, UqlCode::as_str));
    }
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
pub fn method_error_set(d: &MethodDescriptor) -> Vec<&'static str> {
    let mut codes: BTreeSet<_> = d.error_set.iter().copied().collect();
    add_engine_errors(d, &mut codes);
    add_server_errors(d, &mut codes);
    add_owner_errors(d, &mut codes);
    add_typed_handler_errors(d, &mut codes);
    codes.into_iter().collect()
}

/// Whether the canonical method contract permits this served code.
/// An absent method or code fails closed. The table is derived once from the
/// same descriptors and function used by gen_contract.
pub fn method_allows_error(method: &str, code: &str) -> bool {
    static ALLOWED: OnceLock<BTreeMap<&'static str, BTreeSet<&'static str>>> = OnceLock::new();
    ALLOWED
        .get_or_init(|| {
            crate::method_descriptors()
                .map(|descriptor| {
                    (
                        descriptor.id.as_str(),
                        method_error_set(&descriptor).into_iter().collect(),
                    )
                })
                .collect()
        })
        .get(method)
        .is_some_and(|codes| codes.contains(code))
}

#[cfg(test)]
mod tests {
    use super::method_allows_error;

    #[test]
    fn runtime_error_routing_is_method_specific_and_closed() {
        assert!(method_allows_error("CreateGraph", "ACCESS_DENIED"));
        assert!(method_allows_error("CreateGraph", "OPERATION_REDIRECTED"));
        assert!(method_allows_error("CypherQuery", "METHOD_NOT_YET_SERVED"));
        assert!(method_allows_error("Sql", "METHOD_NOT_YET_SERVED"));
        assert!(!method_allows_error("CreateGraph", "UNSUPPORTED_COALITION"));
        assert!(!method_allows_error("NotADeclaredMethod", "ACCESS_DENIED"));
        assert!(method_allows_error("GetNodes", "RESULT_TOO_LARGE"));
        assert!(method_allows_error("GetEdges", "RESULT_TOO_LARGE"));
        assert!(method_allows_error("Uql", "UQL_UNBOUND_PARAMETER"));
        assert!(!method_allows_error("CreateGraph", "UQL_UNBOUND_PARAMETER"));
        assert!(!method_allows_error("GetNodesByLabel", "RESULT_TOO_LARGE"));
        for method in [
            "CreateGraph",
            "AddNode",
            "CypherQuery",
            "GraphQl",
            "SubmitWorkItem",
            "ClaimWorkItem",
            "TsListSeries",
        ] {
            assert!(
                method_allows_error(method, "REPLAY_NONCE_CONSUMED"),
                "{method}"
            );
        }
        assert!(!method_allows_error(
            "TsListSeries",
            "UNSUPPORTED_COALITION"
        ));
        assert!(method_allows_error(
            "KgDelegate",
            "CLUSTER_CONFIGURATION_INVALID"
        ));
        assert!(method_allows_error("KgDelegate", "REPLAY_NONCE_CONSUMED"));
        assert!(method_allows_error("CreateGraph", "RESERVED_GRAPH_NAME"));
        assert!(!method_allows_error("AddNode", "RESERVED_GRAPH_NAME"));
        assert!(!method_allows_error(
            "SubmitWorkItem",
            "CLUSTER_CONFIGURATION_INVALID"
        ));
    }
}
