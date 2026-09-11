#!/usr/bin/env python3
"""Fail closed when the behavior-preserving dispatch module cut drifts."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_module_tree import _rust_code_mask, _rust_comments_mask, read_compiler_family


EXPECTED_PATHS = {
    "src/server/dispatch.rs",
    "src/server/dispatch/change_envelope.rs",
    "src/server/dispatch/consensus.rs",
    "src/server/dispatch/graph_pipeline.rs",
    "src/server/dispatch/graph_pipeline/work_governance.rs",
    "src/server/dispatch/request_boundary.rs",
    "src/server/dispatch/router.rs",
    "src/server/dispatch/router/channels.rs",
    "src/server/dispatch/router/graph_lifecycle.rs",
    "src/server/dispatch/router/identity_access.rs",
    "src/server/dispatch/router/resource_cost.rs",
    "src/server/dispatch/router/service_control.rs",
    "src/server/dispatch/router/source_ingest.rs",
    "src/server/dispatch/sparql_update.rs",
}

REDUNDANT_WRAPPERS = {
    "dispatch_health", "dispatch_parse_file", "dispatch_parse_files",
    "dispatch_index_repository", "dispatch_observe_screen", "dispatch_shutdown",
    "dispatch_unpaged_resource_stats", "dispatch_resource_stats_page",
    "dispatch_create_graph", "dispatch_delete_graph", "dispatch_list_graphs",
    "dispatch_reshard", "dispatch_placement_route", "dispatch_raft_add_learner",
    "dispatch_cluster_members", "dispatch_register_server", "dispatch_create_channel",
    "dispatch_join_channel", "dispatch_leave_channel", "dispatch_close_channel",
    "dispatch_send_message", "dispatch_get_channel_messages", "dispatch_list_channels",
    "dispatch_get_channel_members", "dispatch_register_identity",
    "dispatch_get_identity_from_store", "dispatch_policy_export", "dispatch_rbac_admin",
    "dispatch_apply_multisig_mutation", "dispatch_analytics_job", "dispatch_statechart",
    "dispatch_viz", "dispatch_begin_txn", "dispatch_txn_add_measurement",
    "dispatch_txn_axiom", "dispatch_txn_construct", "dispatch_txn_plan_writeback",
    "dispatch_txn_materialize_belief", "dispatch_blob_begin", "dispatch_kv_get",
    "dispatch_import_sqlite_file", "dispatch_cdc_read", "dispatch_cep_subscribe",
    "dispatch_owl_reason_distributed", "dispatch_apply_change_envelope",
    "dispatch_apply_change_envelopes", "authorize_and_route_served_modality",
    "authorize_and_route_knowledge_stream", "dispatch_get_change_envelope",
    "dispatch_get_content_version", "dispatch_get_change_cursor", "dispatch_nl_query",
    "dispatch_multi_graph_batch_update", "dispatch_op_audit_prove_inclusion",
    "dispatch_op_served_modality", "dispatch_change_env_apply_change_envelope",
    "dispatch_change_env_apply_change_envelopes", "dispatch_change_env_get_change_envelope",
    "dispatch_change_env_get_content_version", "dispatch_change_env_get_change_cursor",
}

ROUTE_OWNERS = {
    "dispatch_service_control_methods": "src/server/dispatch/router/service_control.rs",
    "dispatch_source_ingest_methods": "src/server/dispatch/router/source_ingest.rs",
    "dispatch_resource_cost_methods": "src/server/dispatch/router/resource_cost.rs",
    "dispatch_graph_lifecycle_methods": "src/server/dispatch/router/graph_lifecycle.rs",
    "dispatch_channel_methods": "src/server/dispatch/router/channels.rs",
    "dispatch_identity_and_access_methods": "src/server/dispatch/router/identity_access.rs",
    "route_change_envelope_ops": "src/server/dispatch/change_envelope.rs",
    "route_graph_op_method": "src/server/dispatch/graph_pipeline.rs",
    # `dispatch_op_workitem_mutation` was split in two during the decomposition
    # and this map was never updated, so the gate asserted ownership of a
    # function that exists in neither this tree nor EG main b2ac7b93 -- it had
    # been failing on both. Assert the two real successors instead of dropping
    # the check.
    "dispatch_op_workitem_claim_capability":
        "src/server/dispatch/graph_pipeline/work_governance.rs",
    "dispatch_op_workitem_submission_or_resources":
        "src/server/dispatch/graph_pipeline/work_governance.rs",
}

LEGACY_COALESCER_METHODS = (
    "AddNode", "RemoveNode", "AddEdge", "RemoveEdge", "CompareAndSetNodeFields",
)

# The previous value (e695ad19...) matched NEITHER this tree NOR EG main
# b2ac7b93 (72 predicates, 5264dc68...), so this fingerprint was already stale
# before the RF-020 work and the gate was failing on both. Re-stamped against
# the reviewed candidate: 74 predicates. Two are added relative to main and none
# removed --
#   any(feature = "amqp-wire", "mqtt-wire", "stomp-wire", "mssql-wire", "redis-wire")
#   any(feature = "federation-search", feature = "nl-query")
CFG_FINGERPRINT = "7bb6473c7d61790869f2e44747e1f7cf5eadd05d2faf7fc7d5c5d451769fa62b"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"dispatch decomposition gate failed: {message}")


def sources() -> dict[str, str]:
    family = read_compiler_family(ROOT / "src/server/dispatch.rs", ROOT)
    paths = {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in family.all_paths
    }
    require(set(paths) == EXPECTED_PATHS, "compiler module family or orphan set changed")
    return paths


def function_names(source: str) -> list[str]:
    return re.findall(
        r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)", source
    )


def check_inventory(parts: dict[str, str]) -> None:
    joined = "\n".join(parts.values())
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
    require(len(names) == 330, "named-function inventory changed")
    require(len(re.findall(r"#\[(?:tokio::)?test", joined)) == 48, "test inventory changed")
    require(
        len(re.findall(r"\bassert(?:_eq|_ne)?!", joined)) == 135,
        "assertion inventory changed",
    )
    require(len(REDUNDANT_WRAPPERS) == 60, "wrapper deletion inventory is not exhaustive")
    require(not REDUNDANT_WRAPPERS.intersection(names), "redundant one-arm wrapper returned")
    require("classifier/handler diverged" not in joined, "wrapper sentinel returned")
    for function, owner in ROUTE_OWNERS.items():
        owners = [path for path, source in parts.items() if function in function_names(source)]
        require(owners == [owner], f"{function} ownership changed: {owners}")


def check_cfg_contract(parts: dict[str, str]) -> None:
    joined = "\n".join(parts.values())
    predicates = sorted(
        {
            re.sub(r"\s+", " ", predicate).strip()
            for predicate in re.findall(r"#\[cfg\((.*?)\)\]", joined, re.DOTALL)
        }
    )
    digest = hashlib.sha256("\n".join(predicates).encode()).hexdigest()
    require(len(predicates) == 74 and digest == CFG_FINGERPRINT, "cfg boundary set changed")


def check_routing_and_coalescing(parts: dict[str, str]) -> None:
    graph = parts["src/server/dispatch/graph_pipeline.rs"]
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
        "handlers::wasm_udf::try_handle(", "handlers::federation::try_handle(",
        "handlers::dist_compute::try_handle(",
    )
    for markers in (gateway_order, graph_order, query_order, global_order):
        offsets = [graph.rindex(marker) for marker in markers]
        require(offsets == sorted(offsets), "graph gateway/terminal route order changed")
    pipeline_order = (
        "route_pipeline_compute(&ctx, method)",
        "route_pipeline_surfaces(&ctx, method)",
        "handlers::graph_ops::try_handle(",
    )
    offsets = [graph.rindex(marker) for marker in pipeline_order]
    require(offsets == sorted(offsets), "graph gateway/terminal route order changed")
    require("try_coalesce_write" not in graph, "unreachable legacy coalescer fallback returned")
    gateway = parts.get("src/server/handlers/graph_ops/gateway_graph.rs")
    if gateway is None:
        gateway = (ROOT / "src/server/handlers/graph_ops/gateway_graph.rs").read_text()
    gateway = _rust_code_mask(gateway)
    mutation = _rust_comments_mask((ROOT / "src/server/mutation.rs").read_text())
    routed = mutation.split("pub const GATEWAY_ROUTED: &[&str] = &[", 1)[1].split(
        "\n];", 1
    )[0]
    for method in LEGACY_COALESCER_METHODS:
        require(f"Method::{method}" in gateway, f"legacy coalescer proof lost gateway arm {method}")
        require(f'"{method}"' in routed, f"legacy coalescer method escaped GATEWAY_ROUTED: {method}")


def check_consensus_exhaustiveness(parts: dict[str, str]) -> None:
    consensus = parts["src/server/dispatch/consensus.rs"]
    start = consensus.index("fn native_route_target(")
    end = consensus.index("\n}\n", start) + 2
    route = consensus[start:end]
    require("_ =>" not in route, "native consensus route regained a wildcard fallback")
    require("domain_of" not in "\n".join(parts.values()), "a second dispatch domain registry appeared")


def check_compile_shapes(parts: dict[str, str]) -> None:
    graph = parts["src/server/dispatch/graph_pipeline.rs"]
    router = parts["src/server/dispatch/router.rs"]
    envelopes = parts["src/server/dispatch/change_envelope.rs"]
    require(
        "let method = Method::ServedModality { op: op.clone() };" in graph,
        "ServedModality lost its reconstructed method value",
    )
    require(
        "anchor_seq: Option<u64>" in graph,
        "audit inclusion anchor contract no longer accepts the optional sequence",
    )
    for variant in (
        "TxnAddMeasurement", "TxnAxiom", "TxnConstruct", "TxnPlanWriteback",
        "TxnMaterializeBelief", "OwlReasonDistributed",
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
    require(
        "ApplyChangeEnvelopeCtx" not in envelopes,
        "obsolete change-envelope wrapper context returned",
    )
    require(
        "mint_policy_decision_lease(" in router
        and "&carrier," in router
        and "mint_graph_policy_lease(" not in router,
        "KnowledgeStream lost the integrated policy-decision lease recipe",
    )


def main() -> None:
    parts = sources()
    check_inventory(parts)
    check_cfg_contract(parts)
    check_routing_and_coalescing(parts)
    check_consensus_exhaustiveness(parts)
    check_compile_shapes(parts)
    print("dispatch decomposition gate: OK")


if __name__ == "__main__":
    main()
