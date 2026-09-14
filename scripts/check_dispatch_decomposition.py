#!/usr/bin/env python3
"""Fail closed when the behavior-preserving dispatch module cut drifts."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_lexer import _balanced_span_from, _rust_code_mask, _rust_comments_mask
from rust_module_tree import read_compiler_family

EXPECTED_PATHS = {
    "src/server/dispatch.rs",
    "src/server/dispatch/change_envelope.rs",
    "src/server/dispatch/change_envelope/multi_graph.rs",
    "src/server/dispatch/consensus.rs",
    "src/server/dispatch/consensus/publication.rs",
    "src/server/dispatch/consensus/registry.rs",
    "src/server/dispatch/consensus/replicated.rs",
    "src/server/dispatch/consensus/routing.rs",
    "src/server/dispatch/consensus/sanitization.rs",
    "src/server/dispatch/consensus/transaction.rs",
    "src/server/dispatch/graph_pipeline.rs",
    "src/server/dispatch/graph_pipeline/dispatch_helpers.rs",
    "src/server/dispatch/graph_pipeline/gateway.rs",
    "src/server/dispatch/graph_pipeline/graph_access.rs",
    "src/server/dispatch/graph_pipeline/graph_dispatch.rs",
    "src/server/dispatch/graph_pipeline/modality.rs",
    "src/server/dispatch/graph_pipeline/native_routes.rs",
    "src/server/dispatch/graph_pipeline/pipeline.rs",
    "src/server/dispatch/graph_pipeline/work_governance.rs",
    "src/server/dispatch/request_boundary.rs",
    "src/server/dispatch/request_boundary/authorization.rs",
    "src/server/dispatch/request_boundary/consensus.rs",
    "src/server/dispatch/request_boundary/preflight.rs",
    "src/server/dispatch/request_boundary/saga.rs",
    "src/server/dispatch/request_boundary/screen.rs",
    "src/server/dispatch/router.rs",
    "src/server/dispatch/router/channels.rs",
    "src/server/dispatch/router/control_plane.rs",
    "src/server/dispatch/router/data_plane.rs",
    "src/server/dispatch/router/data_plane_arms.rs",
    "src/server/dispatch/router/graph_lifecycle.rs",
    "src/server/dispatch/router/identity_access.rs",
    "src/server/dispatch/router/lifecycle.rs",
    "src/server/dispatch/router/resource_cost.rs",
    "src/server/dispatch/router/service_control.rs",
    "src/server/dispatch/router/source_ingest.rs",
    "src/server/dispatch/sparql_update.rs",
}

REDUNDANT_WRAPPERS = {
    "dispatch_health",
    "dispatch_parse_file",
    "dispatch_parse_files",
    "dispatch_index_repository",
    "dispatch_observe_screen",
    "dispatch_shutdown",
    "dispatch_unpaged_resource_stats",
    "dispatch_resource_stats_page",
    "dispatch_create_graph",
    "dispatch_delete_graph",
    "dispatch_list_graphs",
    "dispatch_reshard",
    "dispatch_placement_route",
    "dispatch_raft_add_learner",
    "dispatch_cluster_members",
    "dispatch_register_server",
    "dispatch_create_channel",
    "dispatch_join_channel",
    "dispatch_leave_channel",
    "dispatch_close_channel",
    "dispatch_send_message",
    "dispatch_get_channel_messages",
    "dispatch_list_channels",
    "dispatch_get_channel_members",
    "dispatch_register_identity",
    "dispatch_get_identity_from_store",
    "dispatch_policy_export",
    "dispatch_rbac_admin",
    "dispatch_apply_multisig_mutation",
    "dispatch_analytics_job",
    "dispatch_statechart",
    "dispatch_viz",
    "dispatch_begin_txn",
    "dispatch_txn_add_measurement",
    "dispatch_txn_axiom",
    "dispatch_txn_construct",
    "dispatch_txn_plan_writeback",
    "dispatch_txn_materialize_belief",
    "dispatch_blob_begin",
    "dispatch_kv_get",
    "dispatch_import_sqlite_file",
    "dispatch_cdc_read",
    "dispatch_cep_subscribe",
    "dispatch_owl_reason_distributed",
    "dispatch_apply_change_envelope",
    "dispatch_apply_change_envelopes",
    "authorize_and_route_served_modality",
    "authorize_and_route_knowledge_stream",
    "dispatch_get_change_envelope",
    "dispatch_get_content_version",
    "dispatch_get_change_cursor",
    "dispatch_nl_query",
    "dispatch_multi_graph_batch_update",
    "dispatch_op_audit_prove_inclusion",
    "dispatch_op_served_modality",
    "dispatch_change_env_apply_change_envelope",
    "dispatch_change_env_apply_change_envelopes",
    "dispatch_change_env_get_change_envelope",
    "dispatch_change_env_get_content_version",
    "dispatch_change_env_get_change_cursor",
}

ROUTE_OWNERS = {
    "dispatch_service_control_methods": "src/server/dispatch/router/service_control.rs",
    "dispatch_source_ingest_methods": "src/server/dispatch/router/source_ingest.rs",
    "dispatch_resource_cost_methods": "src/server/dispatch/router/resource_cost.rs",
    "dispatch_graph_lifecycle_methods": "src/server/dispatch/router/graph_lifecycle.rs",
    "dispatch_channel_methods": "src/server/dispatch/router/channels.rs",
    "dispatch_identity_and_access_methods": "src/server/dispatch/router/identity_access.rs",
    "route_change_envelope_ops": "src/server/dispatch/change_envelope.rs",
    "route_graph_op_method": "src/server/dispatch/graph_pipeline/native_routes.rs",
    # `dispatch_op_workitem_mutation` was split in two during the decomposition
    # and this map was never updated, so the gate asserted ownership of a
    # function that exists in neither this tree nor EG main b2ac7b93 -- it had
    # been failing on both. Assert the two real successors instead of dropping
    # the check.
    "dispatch_op_workitem_claim_capability": "src/server/dispatch/graph_pipeline/work_governance.rs",
    "dispatch_op_workitem_submission_or_resources": "src/server/dispatch/graph_pipeline/work_governance.rs",
}

ROUTE_CALLERS = {
    "dispatch_service_control_methods": "src/server/dispatch/router.rs",
    "dispatch_source_ingest_methods": "src/server/dispatch/router.rs",
    "dispatch_resource_cost_methods": "src/server/dispatch/router.rs",
    "dispatch_graph_lifecycle_methods": "src/server/dispatch/router.rs",
    "dispatch_channel_methods": "src/server/dispatch/router.rs",
    "dispatch_identity_and_access_methods": "src/server/dispatch/router.rs",
    "route_change_envelope_ops": "src/server/dispatch/graph_pipeline/native_routes.rs",
    "route_graph_op_method": "src/server/dispatch/graph_pipeline/graph_dispatch.rs",
    "dispatch_op_workitem_claim_capability": "src/server/dispatch/graph_pipeline/native_routes.rs",
    "dispatch_op_workitem_submission_or_resources": "src/server/dispatch/graph_pipeline/native_routes.rs",
}

LEGACY_COALESCER_METHODS = (
    "AddNode",
    "RemoveNode",
    "AddEdge",
    "RemoveEdge",
    "CompareAndSetNodeFields",
)

# The previous value (e695ad19...) matched NEITHER this tree NOR EG main
# b2ac7b93 (72 predicates, 5264dc68...), so this fingerprint was already stale
# before the RF-020 work and the gate was failing on both. Re-stamped against
# the reviewed candidate before the KISS module split: 74 predicates. The split
# preserves those predicates and adds two module-boundary cfg declarations, so
# the compiler family then contained 76. Integration removes the sole
# `all(feature = "redb", feature = "security")` predicate: every SPARQL HTTP
# helper now also requires `feature = "sparql-http"`, so that over-broad
# configuration is no longer compiler-visible. The two original additions
# relative to main remain --
#   any(feature = "amqp-wire", "mqtt-wire", "stomp-wire", "mssql-wire", "redis-wire")
#   any(feature = "federation-search", feature = "nl-query")
CFG_FINGERPRINT = "35aeaf654089d9dc52eedef7da795bf39805eba94ceecc42bdb0c5f9c45304d8"
PRODUCTION_FUNCTION_COUNT = 355
PRODUCTION_FUNCTION_DIGEST = (
    "c425d7d9ec01712f70b9fdc81d750f274095a923fc467ddf41b76f9b32ea326d"
)
COMPILER_FUNCTION_COUNT = 440
COMPILER_FUNCTION_DIGEST = (
    "af06cb2a46a9cc9bbb0910a86f2ea0a6cb6c2e067ccd69b603504029e223265e"
)
TEST_FUNCTION_COUNT = 52
TEST_FUNCTION_DIGEST = (
    "85b849ca6c25c0196bb1bb3bd7104f35b210871e072d6d1b007e88ada1974024"
)
ASSERTION_COUNT = 145
ASSERTION_DIGEST = "ac9bd626e650acfb947510f8b1867837379a6a72726857d75316f50c46ada2c6"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"dispatch decomposition gate failed: {message}")


def sources() -> dict[str, str]:
    family = read_compiler_family(ROOT / "src/server/dispatch.rs", ROOT)
    paths = {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in family.all_paths
    }
    require(
        set(paths) == EXPECTED_PATHS, "compiler module family or orphan set changed"
    )
    return paths


def compiler_views() -> tuple[str, str]:
    """Return compiler-resolved production and test-enabled dispatch views."""

    family = read_compiler_family(ROOT / "src/server/dispatch.rs", ROOT)
    return family.production, family.with_tests


def function_names(source: str) -> list[str]:
    """Named functions in Rust code, excluding comments and every literal kind."""

    return re.findall(
        r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)",
        _rust_code_mask(source),
    )


def test_function_names(source: str) -> list[str]:
    """Functions carrying a compiler-visible Rust test attribute."""

    code = _rust_code_mask(source)
    return re.findall(
        r"#\s*\[\s*(?:tokio::)?test(?:\s*\([^]]*\))?\s*\]\s*"
        r"(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)",
        code,
    )


def assertion_inventory(source: str) -> list[str]:
    """Normalized compiler-visible assert macro invocations."""

    code = _rust_code_mask(source)
    assertions: list[str] = []
    closing = {"(": ")", "[": "]", "{": "}"}
    pattern = re.compile(r"\bassert(?:_eq|_ne)?!\s*([([{])")
    for match in pattern.finditer(code):
        opener = match.end() - 1
        closer = _balanced_span_from(code, opener, code[opener], closing[code[opener]])
        assertions.append(re.sub(r"\s+", " ", code[match.start() : closer + 1]).strip())
    return assertions


def inventory_digest(items: list[str]) -> str:
    """Stable digest retaining duplicate inventory entries."""

    return hashlib.sha256("\n".join(sorted(items)).encode()).hexdigest()


def check_compiler_inventory(production: str, with_tests: str) -> None:
    """Pin exact compiler-reachable function, test, and assertion inventories."""

    production_names = function_names(production)
    compiler_names = function_names(with_tests)
    tests = test_function_names(with_tests)
    assertions = assertion_inventory(with_tests)
    require(
        len(production_names) == PRODUCTION_FUNCTION_COUNT
        and inventory_digest(production_names) == PRODUCTION_FUNCTION_DIGEST,
        "production function inventory changed",
    )
    require(
        len(compiler_names) == COMPILER_FUNCTION_COUNT
        and inventory_digest(compiler_names) == COMPILER_FUNCTION_DIGEST,
        "named-function inventory changed",
    )
    require(
        len(tests) == TEST_FUNCTION_COUNT
        and inventory_digest(tests) == TEST_FUNCTION_DIGEST,
        "test inventory changed",
    )
    require(
        len(assertions) == ASSERTION_COUNT
        and inventory_digest(assertions) == ASSERTION_DIGEST,
        "assertion inventory changed",
    )


def check_route_calls(parts: dict[str, str]) -> None:
    for function, caller in ROUTE_CALLERS.items():
        code = _rust_code_mask(parts[caller])
        require(
            re.search(rf"\b{re.escape(function)}\s*\(", code) is not None,
            f"{function} call missing from {caller}",
        )


def check_optional_compiler_inventory(views: tuple[str, str] | None) -> None:
    """Check the exact compiler inventory when the caller supplied its views."""

    if views is not None:
        check_compiler_inventory(*views)


def check_inventory(
    parts: dict[str, str], views: tuple[str, str] | None = None
) -> None:
    joined = _rust_code_mask("\n".join(parts.values()))
    names = function_names(joined)
    # 324 -> 331. Seven functions were added to the dispatch compiler family and
    # none were removed (verified by diffing the name inventory against EG main
    # b2ac7b93, not by trusting the count):
    #   dispatch_agent_library_methods          -- RF-020 Agent Library routing
    #   is_replicated_apply                     -- raft apply classification
    #   propose_native_mutation (two cfg arms)  -- native mutation proposal
    #   replicated_placement_authority          -- replicated placement/fencing
    #   submit_consensus_job_publication_commit    -- analytics-job publication
    #   submit_consensus_job_publication_response  -- analytics-job publication
    # 331 -> 330. One function was REMOVED by the dupehound consolidation:
    #   submit_context_matches_authority -- its four-field tenant/agent/audience/
    #   policy_version comparison was the same one `kg-delegate` ran, so both
    #   boundaries now call the single `handlers::delegation::
    #   context_matches_verified_authority`. The CHECK is preserved (see
    #   `validate_submit_context`); only the duplicate definition is gone.
    require(
        len(REDUNDANT_WRAPPERS) == 60, "wrapper deletion inventory is not exhaustive"
    )
    require(
        not REDUNDANT_WRAPPERS.intersection(names), "redundant one-arm wrapper returned"
    )
    require("classifier/handler diverged" not in joined, "wrapper sentinel returned")
    for function, owner in ROUTE_OWNERS.items():
        owners = [
            path for path, source in parts.items() if function in function_names(source)
        ]
        require(owners == [owner], f"{function} ownership changed: {owners}")
    check_route_calls(parts)
    check_optional_compiler_inventory(views)


def check_cfg_contract(parts: dict[str, str]) -> None:
    joined = "\n".join(parts.values())
    predicates = sorted(
        {
            re.sub(r"\s+", " ", predicate).strip()
            for predicate in re.findall(r"#\[cfg\((.*?)\)\]", joined, re.DOTALL)
        }
    )
    digest = hashlib.sha256("\n".join(predicates).encode()).hexdigest()
    require(
        len(predicates) == 75 and digest == CFG_FINGERPRINT, "cfg boundary set changed"
    )


def family_source(parts: dict[str, str], root: str) -> str:
    prefix = root[:-3] if root.endswith(".rs") else root
    return "\n".join(
        source
        for path, source in parts.items()
        if path == root or path.startswith(f"{prefix}/")
    )


def check_route_order(graph: str) -> None:
    graph = _rust_code_mask(graph)
    # `handlers::tts::try_handle(` was dropped: `src/server/handlers/tts.rs` was
    # DELETED in 7469acff and the TTS surface moved to the modality handler, so
    # this marker had named a symbol present in neither this tree nor EG main
    # b2ac7b93 -- the gate was crashing with `ValueError: substring not found`
    # rather than checking an order. The gateway stage now ends at `finance`.
    gateway_order = (
        "handlers::graph_ops::try_handle_gateway(",
        "handlers::finance::try_handle(",
    )
    graph_order = (
        "handlers::datascience::try_handle(",
        "handlers::mining::try_handle(",
        "handlers::graphlearn::try_handle(",
        "handlers::pipeline::try_handle(",
    )
    query_order = ("route_query_gateway(", "route_rdf_gateway(")
    global_order = (
        "handlers::wasm_udf::try_handle(",
        "handlers::federation::try_handle(",
        "handlers::dist_compute::try_handle(",
    )
    for markers in (gateway_order, graph_order, query_order, global_order):
        offsets = [graph.rindex(marker) for marker in markers]
        require(
            offsets == sorted(offsets), "graph gateway/terminal route order changed"
        )
    pipeline_order = (
        "route_pipeline_compute(&ctx, method)",
        "route_pipeline_surfaces(&ctx, method)",
        "handlers::graph_ops::try_handle(",
    )
    offsets = [graph.rindex(marker) for marker in pipeline_order]
    require(offsets == sorted(offsets), "graph gateway/terminal route order changed")


def check_post_lock_route_order(native_routes: str) -> None:
    native_routes = _rust_code_mask(native_routes)
    post_lock_order = (
        "route_change_envelope_ops(",
        "route_native_store_ops(",
        "route_graph_authority_surfaces(",
    )
    offsets = [native_routes.rindex(marker) for marker in post_lock_order]
    require(offsets == sorted(offsets), "post-lock graph router order changed")


def check_legacy_coalescer(parts: dict[str, str]) -> None:
    graph_family = family_source(parts, "src/server/dispatch/graph_pipeline.rs")
    require(
        "try_coalesce_write" not in graph_family,
        "unreachable legacy coalescer fallback returned",
    )
    gateway = parts.get("src/server/handlers/graph_ops/gateway_graph.rs")
    if gateway is None:
        gateway = read_compiler_family(
            ROOT / "src/server/handlers/graph_ops/gateway_graph.rs", ROOT
        ).production
    gateway = _rust_code_mask(gateway)
    mutation = _rust_comments_mask(
        read_compiler_family(ROOT / "src/server/mutation.rs", ROOT).production
    )
    routed = mutation.split("pub const GATEWAY_ROUTED: &[&str] = &[", 1)[1].split(
        "\n];", 1
    )[0]
    for method in LEGACY_COALESCER_METHODS:
        require(
            f"Method::{method}" in gateway,
            f"legacy coalescer proof lost gateway arm {method}",
        )
        require(
            f'"{method}"' in routed,
            f"legacy coalescer method escaped GATEWAY_ROUTED: {method}",
        )


def check_routing_and_coalescing(parts: dict[str, str]) -> None:
    graph = parts["src/server/dispatch/graph_pipeline/pipeline.rs"]
    check_route_order(graph)
    check_post_lock_route_order(
        parts["src/server/dispatch/graph_pipeline/native_routes.rs"]
    )
    check_legacy_coalescer(parts)


def check_consensus_exhaustiveness(parts: dict[str, str]) -> None:
    consensus = parts["src/server/dispatch/consensus/routing.rs"]
    start = consensus.index("fn native_route_target(")
    end = consensus.index("\n}\n", start) + 2
    route = _rust_code_mask(consensus[start:end])
    require("_ =>" not in route, "native consensus route regained a wildcard fallback")
    require(
        re.search(r"Some\(\s*other\s*\)\s*=>\s*request_graph\.to_string\(\)", route)
        is None,
        "native consensus route regained a fail-open catch-all",
    )
    require(
        "domain_of" not in "\n".join(parts.values()),
        "a second dispatch domain registry appeared",
    )


def check_graph_compile_shapes(graph: str) -> None:
    require(
        "let method = Method::ServedModality { op: op.clone() };" in graph,
        "ServedModality lost its reconstructed method value",
    )
    require(
        "anchor_seq: Option<u64>" in graph,
        "audit inclusion anchor contract no longer accepts the optional sequence",
    )


def check_router_compile_shapes(router: str) -> None:
    for variant in (
        "TxnAddMeasurement",
        "TxnAxiom",
        "TxnConstruct",
        "TxnPlanWriteback",
        "TxnMaterializeBelief",
        "OwlReasonDistributed",
    ):
        require(
            f"method @ (Method::{variant}" not in router,
            f"{variant} regained an unnecessary at-pattern parenthesis",
        )
    require(
        "Method::NlQuery { text, graph }" in router
        and "Method::NlQuery { text, graph }," in router,
        "NlQuery no longer destructures and reconstructs without borrow/move overlap",
    )


def check_envelope_compile_shapes(envelopes: str) -> None:
    require(
        "ApplyChangeEnvelopeCtx" not in envelopes,
        "obsolete change-envelope wrapper context returned",
    )


def check_knowledge_stream_compile_shape(router: str) -> None:
    require(
        "mint_policy_decision_lease(" in router
        and "&carrier," in router
        and "mint_graph_policy_lease(" not in router,
        "KnowledgeStream lost the integrated policy-decision lease recipe",
    )


def check_compile_shapes(parts: dict[str, str]) -> None:
    graph = family_source(parts, "src/server/dispatch/graph_pipeline.rs")
    router = family_source(parts, "src/server/dispatch/router.rs")
    envelopes = family_source(parts, "src/server/dispatch/change_envelope.rs")
    check_graph_compile_shapes(graph)
    check_router_compile_shapes(router)
    check_envelope_compile_shapes(envelopes)
    check_knowledge_stream_compile_shape(router)


def main() -> None:
    parts = sources()
    check_inventory(parts, compiler_views())
    check_cfg_contract(parts)
    check_routing_and_coalescing(parts)
    check_consensus_exhaustiveness(parts)
    check_compile_shapes(parts)
    print("dispatch decomposition gate: OK")


if __name__ == "__main__":
    main()
