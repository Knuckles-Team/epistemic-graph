"""CI entry point for the current-only persisted mutation contract gate."""

from __future__ import annotations

import copy
import importlib.util
import re
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


def _gate_module():
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_persisted_mutation_contract.py"
    spec = importlib.util.spec_from_file_location("persisted_mutation_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _m1_sources(module):
    # crates/eg-mutation-store is deleted; module.mutation_kernel_source() is
    # its current-only successor (the union of eg-storage's StorageKernel
    # and eg-transaction's MutationKernel module trees). See that helper's
    # docstring in scripts/check_persisted_mutation_contract.py.
    return (
        module.read_module_tree("crates/eg-types/src/mutation_batch.rs"),
        module.mutation_kernel_source(),
        module.read_module_tree(
            "crates/eg-types/src/mutation_batch.rs", include_tests=True
        ),
        module.mutation_kernel_source(include_tests=True),
        module.read_module_tree("src/graph_delta.rs"),
    )


def _remove_all_gateway_router_occurrences(module, sources, arm: str) -> None:
    """Delete one method variant from every compiler-family router child.

    The graph gateway is now assembled from a facade helper plus private route
    children.  A variant can therefore occur in a classifier and its live match
    arm, or in the facade helper and the child.  Mutating only the first textual
    occurrence would leave a duplicate marker that masks a missing ownership row.
    Rebuild the assembled router from the exact declared children and remove all
    occurrences so the perturbation remains fail-closed.
    """

    assembled = sources["graph_gateway_routes"]
    assembled_count = assembled.count(arm)
    occurrence_count = 0
    for filename in module._GRAPH_GATEWAY_ROUTER_FILES:
        child = module.read(f"src/server/handlers/graph_ops/{filename}")
        count = child.count(arm)
        if count == 0:
            continue
        occurrence_count += count
        broken_child = child.replace(arm, "Method::RemovedGatewayArm {")
        assert assembled.count(child) == 1
        assembled = assembled.replace(child, broken_child, 1)

    assert occurrence_count > 0
    assert occurrence_count == assembled_count
    assert assembled.count(arm) == 0
    sources["graph_gateway_routes"] = assembled


def test_persisted_mutation_contract_gate() -> None:
    _gate_module().main()


def test_persisted_contract_follows_mutation_batch_facade_tree() -> None:
    module = _gate_module()
    source = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")

    assert "pub struct MutationOperation" in source
    assert "pub domain: DurabilityDomain" in source


def test_graph_ops_facade_declares_complete_non_orphan_module_tree() -> None:
    module = _gate_module()
    facade_path = "src/server/handlers/graph_ops.rs"
    facade = module.read(facade_path)
    family = module.read_compiler_family(facade_path)

    expected_children = {
        "algorithms.rs",
        "broker.rs",
        "edges.rs",
        "gateway.rs",
        "gateway_broker.rs",
        "gateway_graph.rs",
        "gateway_graph_routes.rs",
        "gateway_mining.rs",
        "gateway_mining_derived.rs",
        "gateway_mining_ml.rs",
        "hierarchy.rs",
        "memory.rs",
        "nodes.rs",
        "semantic.rs",
        "subgraph.rs",
        "terminal.rs",
        "union.rs",
    }
    child_paths = {
        path.name for path in family.all_paths if path.parent.name == "graph_ops"
    }
    assert child_paths == expected_children
    assert len(facade.splitlines()) <= 80
    assert not re.search(r"(?m)^\s*(?:pub\([^)]*\)\s+)?(?:async\s+)?fn\s+", facade)
    assert re.findall(r"(?m)^pub\(crate\) use ([^;]+);$", facade) == [
        "gateway::try_handle_gateway",
        "terminal::try_handle",
    ]
    gateway = module._rust_comments_mask(
        module.read("src/server/handlers/graph_ops/gateway.rs")
    )
    gateway_signature = gateway[
        gateway.index("pub(crate) async fn try_handle_gateway") : gateway.index(
            "{", gateway.index("pub(crate) async fn try_handle_gateway")
        )
    ]
    assert re.sub(r"\s+", " ", gateway_signature).strip() == (
        "pub(crate) async fn try_handle_gateway( req_id: u64, caller: Option<&str>, "
        "attempt_nonce: Option<eg_types::contract::Nonce>, idempotency_key: &str, "
        "tenant_scope: &str, graph_name: &str, core: &Arc<GraphCore>, "
        "materialization_manifest: Option< "
        "&Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>, >, "
        "read_authority: Option<&GraphReadAuthority>, "
        "persistence: Option<&Arc<dyn PersistenceBackend>>, "
        '#[cfg(feature = "streaming")] '
        "cdc: Option<&Arc<crate::server::cdc::CdcHub>>, "
        "write_coalescer: Option< "
        "&Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>, >, "
        "authz_ctx: Option<&GatewayAuthzCtx>, "
        '#[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))] '
        "tsdb_store: Option< "
        "&Arc<eg_tsdb::store::SeriesStore>, >, method: Method, ) "
        "-> Result<Response, Method>"
    )
    terminal = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/terminal.rs")
    )
    terminal_signature = terminal[
        terminal.index("pub(crate) async fn try_handle") : terminal.index(
            "{", terminal.index("pub(crate) async fn try_handle")
        )
    ]
    assert re.sub(r"\s+", " ", terminal_signature).strip() == (
        "pub(crate) async fn try_handle( state: &Arc<RwLock<ServerState>>, "
        "req_id: u64, _caller: Option<&str>, graph_name: &str, "
        "read_authority: &GraphReadAuthority, core: Arc<GraphCore>, "
        "method: Method, ) -> Response"
    )
    production_code = module._rust_code_mask(family.production)
    assert len(module._METHOD_VARIANT.findall(production_code)) == 260
    assert "under_cap_returns_no_error_so_data_is_served" not in family.production
    assert "under_cap_returns_no_error_so_data_is_served" in family.with_tests


def test_graph_ops_gateway_families_contribute_real_method_arms() -> None:
    module = _gate_module()
    markers = {
        "gateway_graph_routes.rs": (
            "Method::AddNode",
            "commit_gateway_coalescable",
        ),
        "gateway_broker.rs": ("Method::Publish", "commit_gateway"),
        "gateway_mining.rs": ("Method::MineAssociate", "commit_conditional_mutation"),
        "gateway_mining_ml.rs": (
            "Method::GraphLearnFit",
            "Method::MiningPipelineTrain",
        ),
    }
    for filename, required in markers.items():
        source = module._rust_code_mask(
            module.read(f"src/server/handlers/graph_ops/{filename}")
        )
        assert all(marker in source for marker in required)


def test_graph_ops_domain_router_order_is_pinned() -> None:
    module = _gate_module()
    gateway = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/gateway.rs")
    )
    gateway_order = (
        "gateway_graph::try_handle",
        "gateway_broker::try_handle",
        "gateway_mining_ml::try_handle",
        "gateway_mining::try_handle",
    )
    assert [gateway.index(marker) for marker in gateway_order] == sorted(
        gateway.index(marker) for marker in gateway_order
    )

    terminal = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/terminal.rs")
    )
    terminal_order = (
        "try_handle_node_gateway_writes",
        "try_handle_node_reads",
        "try_handle_node_gateway_claims",
        "try_handle_broker_exchange",
        "try_handle_broker_consumption",
        "try_handle_streams",
        "try_handle_publisher_confirms",
        "try_handle_memory_maintenance",
        "try_handle_summary_reads",
        "try_handle_scene_graph",
        "try_handle_trajectory_memory",
        "try_handle_node_batch",
        "try_handle_semantic_compute",
        "try_handle_edge_writes",
        "try_handle_edge_reads",
        "try_handle_graph_counts",
        "try_handle_graph_algorithms",
        "try_handle_lifecycle_serialization",
        "try_handle_neighbor_queries",
        "try_handle_centrality_algorithms",
        "try_handle_community_algorithms",
        "try_handle_hierarchy_visualization",
        "try_handle_lifecycle_context",
        "try_handle_ledger",
        "try_handle_subgraph_reads",
        "try_handle_cross_graph_union",
        "try_handle_subgraph_comparison",
    )
    assert [terminal.index(marker) for marker in terminal_order] == sorted(
        terminal.index(marker) for marker in terminal_order
    )


def test_terminal_gateway_guards_have_reachable_control_flow() -> None:
    module = _gate_module()
    guarded = {
        "nodes.rs": (
            "try_handle_node_gateway_writes",
            "try_handle_node_gateway_claims",
        ),
        "edges.rs": ("try_handle_edge_writes",),
        "memory.rs": ("try_handle_memory_maintenance",),
    }
    for filename, functions in guarded.items():
        source = module.read(f"src/server/handlers/graph_ops/{filename}")
        for function in functions:
            body = module._rust_code_mask(module._function(source, function))
            assert "ControlFlow::Break(match" not in body
            assert "other => ControlFlow::Continue(other)" in body

    broker = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/broker.rs")
    )
    assert "ControlFlow::Break(match $method" not in broker
    assert "match $method" in broker
    assert (
        "allow(unreachable_code)"
        not in module.read_compiler_family(
            "src/server/handlers/graph_ops.rs"
        ).with_tests
    )


def test_decode_json_object_has_one_visible_owner_and_two_consumers() -> None:
    module = _gate_module()
    family = module.read_compiler_family("src/server/handlers/graph_ops.rs")
    family_code = module._rust_code_mask(family.production)
    gateway = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/gateway_graph.rs")
    )
    routes = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/gateway_graph_routes.rs")
    )
    semantic = module._rust_code_mask(
        module.read("src/server/handlers/graph_ops/semantic.rs")
    )

    assert len(re.findall(r"\bfn\s+decode_json_object\s*\(", family_code)) == 1
    assert "pub(super) fn decode_json_object(" in gateway
    assert gateway.count("decode_json_object(") == 1
    assert routes.count("decode_json_object(") == 3
    assert semantic.count("super::gateway_graph::decode_json_object(") == 1


def test_mutation_receipts_bind_typed_scope_identity() -> None:
    module = _gate_module()
    contract, native_store, contract_tests, native_store_tests, row_delta = _m1_sources(
        module
    )

    module.check_m1_mutation_identity(
        contract, native_store, contract_tests, native_store_tests, row_delta
    )
    assert "pub identity: MutationScopeIdentity" in contract
    assert "pub version_expectation: VersionExpectation" in contract
    assert "pub const MUTATION_BATCH_VERSION: u16 = 1;" in contract


def test_semantic_coordinator_has_one_closed_native_domain() -> None:
    module = _gate_module()
    contract = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")

    assert "SemanticIndex = 8" in contract
    assert '"analytics_job",\n    "semantic_index",\n    "broker"' in contract
    assert "VectorIndex" not in contract
    assert "AnnIndex" not in contract
    assert "TextIndex" not in contract


def test_m1_freezes_exact_semantic_binding_and_activation_owners() -> None:
    module = _gate_module()
    authorities = {authority for authority, _ in module._M1_SEMANTIC_MARKERS}

    assert authorities == {
        "query.catalog-binding",
        "query.binding-validation",
        "server.generation-coordinator",
    }
    module._check_m1_semantic_inventory()


def test_m1_semantic_inventory_rejects_storage_bypass() -> None:
    module = _gate_module()
    sources = {
        "query": "\n".join(
            (
                "pub struct BindingRequest",
                "pub struct EmbeddingBinding",
                "pub fn bind(",
                "pub fn validate_binding(",
            )
        ),
        "server": "\n".join(
            (
                "pub fn activate_one(",
                "maybe_activate_after_write(",
                "eg_transaction::begin_partial();",
            )
        ),
    }
    with pytest.raises(
        SystemExit, match="semantic authority bypasses the mutation owner"
    ):
        module._check_m1_semantic_inventory(sources)


def test_live_mutation_inventory_contract() -> None:
    module = _gate_module()
    module.check_mutation_inventory(module.mutation_inventory_sources())


@pytest.mark.parametrize(
    "arm",
    (
        "Method::CreateNodeIfAbsent {",
        "Method::DeleteExchange {",
        "Method::MineSequence {",
    ),
)
def test_gateway_inventory_rejects_deleted_live_router_arm(arm: str) -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    _remove_all_gateway_router_occurrences(module, sources, arm)

    with pytest.raises(SystemExit, match="served gateway ownership"):
        module.check_mutation_inventory(sources)


def test_gateway_inventory_rejects_comment_spoof_for_deleted_router_arm() -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    arm = "Method::CreateNodeIfAbsent {"
    _remove_all_gateway_router_occurrences(module, sources, arm)
    sources["graph_gateway_routes"] += f"\n// {arm}\n"

    with pytest.raises(SystemExit, match="served gateway ownership"):
        module.check_mutation_inventory(sources)


def test_every_work_item_variant_is_required_by_direct_classifier() -> None:
    module = _gate_module()
    base_sources = module.mutation_inventory_sources()
    families = base_sources["method_families"]
    for variant in (
        "SubmitWorkItem",
        "SubmitWorkItems",
        "ClaimWorkItem",
        "RenewWorkItemLease",
        "CommitWorkItemResult",
        "CancelWorkItem",
        "DeferWorkItem",
        "CasWorkItemMetadata",
    ):
        sources = copy.deepcopy(base_sources)
        marker = f"Self::{variant} {{ .. }}"
        assert families.count(marker) == 1
        sources["method_families"] = families.replace(
            marker, "Self::RemovedWorkItem { .. }", 1
        )

        with pytest.raises(
            SystemExit, match="current WorkItem lifecycle|live durable classifier"
        ):
            module.check_mutation_inventory(sources)


@pytest.mark.parametrize(
    "replacement",
    (
        "/* Method::SubmitWorkItem { .. } Method::SubmitWorkItems { .. } "
        "Method::ClaimWorkItem { .. } Method::RenewWorkItemLease { .. } "
        "Method::CommitWorkItemResult { .. } Method::CancelWorkItem { .. } "
        "Method::DeferWorkItem { .. } Method::CasWorkItemMetadata { .. } */ false",
        "if dynamic { matches!(method, Method::SubmitWorkItem { .. }) } else { false }",
        "matches!(method, Method::SubmitWorkItem { .. }) "
        "|| matches!(method, Method::SubmitWorkItems { .. })",
        "matches!(candidate, Method::SubmitWorkItem { .. })",
        "matches!(candidate.write_family(), "
        "Some(MethodWriteFamily::WorkItemSubmission "
        "| MethodWriteFamily::WorkItemLease))",
        "matches!(method.write_family(), Some(MethodWriteFamily::WorkItemSubmission)) "
        "|| matches!(method.write_family(), Some(MethodWriteFamily::WorkItemLease))",
        "!matches!(method.write_family(), "
        "Some(MethodWriteFamily::WorkItemSubmission "
        "| MethodWriteFamily::WorkItemLease))",
        "matches!(method.write_family(), "
        "None | Some(MethodWriteFamily::WorkItemSubmission "
        "| MethodWriteFamily::WorkItemLease))",
    ),
)
def test_work_item_classifier_rejects_nondirect_or_spoofed_shapes(
    replacement: str,
) -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    classifier = module._function(sources["mutation_batch"], "is_work_item_method")
    sources["mutation_batch"] = sources["mutation_batch"].replace(
        classifier, replacement, 1
    )

    with pytest.raises(SystemExit, match="is_work_item_method must"):
        module.check_mutation_inventory(sources)


def test_work_item_family_selection_rejects_unknown_or_spoofed_families() -> None:
    module = _gate_module()
    base_sources = module.mutation_inventory_sources()
    classifier = module._function(base_sources["mutation_batch"], "is_work_item_method")

    unknown = copy.deepcopy(base_sources)
    unknown["mutation_batch"] = unknown["mutation_batch"].replace(
        classifier,
        "matches!(method.write_family(), "
        "Some(MethodWriteFamily::WorkItemSubmission | MethodWriteFamily::Unreviewed))",
        1,
    )
    with pytest.raises(SystemExit, match="unknown Method family"):
        module.check_mutation_inventory(unknown)

    spoofed = copy.deepcopy(base_sources)
    spoofed["method_families"] = spoofed["method_families"].replace(
        "| Self::SubmitWorkItems { .. }",
        "/* | Self::SubmitWorkItems { .. } */",
        1,
    )
    assert spoofed["method_families"] != base_sources["method_families"]
    with pytest.raises(SystemExit, match="current WorkItem lifecycle"):
        module.check_mutation_inventory(spoofed)


def test_internal_graph_commit_lock_is_fail_closed() -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    body = module._function(
        sources["mutation_batch"], "commit_internal_graph_methods_with_nonce_mode"
    )
    assert "lock_graph(request.graph).await" in body
    broken = body.replace(
        "lock_graph(request.graph).await", "lock_graph_removed(request.graph).await", 1
    )
    sources["mutation_batch"] = sources["mutation_batch"].replace(body, broken, 1)

    with pytest.raises(SystemExit, match="internal graph commits must acquire"):
        module.check_mutation_inventory(sources)


def test_internal_graph_commit_carries_state_msgpack_to_authority() -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    body = module._function(sources["mutation_batch"], "commit_prepared_internal_graph")
    assert "state_msgpack,\n        descriptor," in body
    assert "commit_mutation_batch_state(\n" in body
    module._check_internal_graph_state_payload(sources["mutation_batch"])

    broken = body.replace(
        "        state_msgpack,\n        descriptor,", "        descriptor,", 1
    )
    sources["mutation_batch"] = sources["mutation_batch"].replace(body, broken, 1)

    with pytest.raises(SystemExit, match="must carry state_msgpack"):
        module._check_internal_graph_state_payload(sources["mutation_batch"])


def test_mutation_batch_source_tree_contains_decomposed_authorities() -> None:
    module = _gate_module()
    source = module.read_module_tree("src/server/mutation_batch.rs")
    for marker in (
        "fn lower_canonical_operation(",
        "fn commit_internal_graph_methods(",
        "fn compile_methods(",
        "fn opaque_state_operation(",
    ):
        assert marker in source


def test_durable_apply_source_union_contains_recursive_lanes() -> None:
    module = _gate_module()
    source = module.mutation_inventory_sources()["durable_apply"]
    for marker in (
        "pub fn is_durable_mutation(",
        "pub fn apply(",
        "Method::AddNode",
        "Method::AddEdge",
        "Method::CreateSummaryNode",
        "Method::AddSceneObject",
        "Method::DeclareExchange",
        "Method::PublishIdempotent",
    ):
        assert marker in source


def test_durable_classifier_reads_family_variants_from_code_only() -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    module.check_mutation_inventory(sources)

    removed = copy.deepcopy(sources)
    removed["method_families"] = removed["method_families"].replace(
        "| Self::AppendStep { .. }", "", 1
    )
    assert removed["method_families"] != sources["method_families"]
    with pytest.raises(SystemExit, match="AppendStep"):
        module.check_mutation_inventory(removed)

    spoofed = copy.deepcopy(removed)
    spoofed["method_families"] = spoofed["method_families"].replace(
        "| Self::StartTrajectory { .. }",
        "| Self::StartTrajectory { .. } // | Self::AppendStep { .. }",
        1,
    )
    assert spoofed["method_families"] != removed["method_families"]
    with pytest.raises(SystemExit, match="AppendStep"):
        module.check_mutation_inventory(spoofed)


def test_native_command_catalog_rejects_drift_and_comment_spoofs() -> None:
    module = _gate_module()
    source = module.read_compiler_family("src/raft/mod.rs").production
    entry = "            record EvictLRU => GraphState,\n"
    assert entry in source
    assert len(module._native_method_catalog(source)) == 100

    with pytest.raises(SystemExit, match="100 entries"):
        module._native_method_catalog(source.replace(entry, "", 1))
    with pytest.raises(SystemExit, match="duplicate entry"):
        module._native_method_catalog(source.replace(entry, entry + entry, 1))
    with pytest.raises(SystemExit, match="100 entries"):
        module._native_method_catalog(
            source.replace(entry, f"            // {entry.strip()}\n", 1)
        )
    with pytest.raises(SystemExit, match="domain partition drifted"):
        module._native_method_catalog(
            source.replace(entry, entry.replace("GraphState", "Transaction"), 1)
        )
    with pytest.raises(SystemExit, match="share one catalog"):
        module._native_method_catalog(
            source.replace(
                "native_method_catalog!(declare_native_domain_classifier);", "", 1
            )
        )

    arm_tail = "        }\n    };\n}\n\nmacro_rules! declare_native_consensus_methods"
    assert arm_tail in source
    for delimiter in (";", ","):
        unused_arm = (
            "        }\n"
            "    };\n"
            "    ($unused:literal) => {\n"
            f"{entry}"
            f"    }}{delimiter}\n"
            "}\n\n"
            "macro_rules! declare_native_consensus_methods"
        )
        moved_to_unused_arm = source.replace(entry, "", 1).replace(
            arm_tail, unused_arm, 1
        )
        with pytest.raises(SystemExit, match="exactly one.*consumer:ident arm"):
            module._native_method_catalog(moved_to_unused_arm)


def test_native_command_payload_limit_has_one_owner() -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    module._check_coordinator_limits(sources)

    sources["raft"] += (
        "\nconst MAX_REPLICATED_COMMAND_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;\n"
    )
    with pytest.raises(SystemExit, match="one owner"):
        module._check_coordinator_limits(sources)

    clean = module.mutation_inventory_sources()
    declaration = (
        "pub(super) const MAX_REPLICATED_COMMAND_PAYLOAD_BYTES: usize = "
        "128 * 1024 * 1024;"
    )
    assert declaration in clean["raft"]
    declaration_comment = copy.deepcopy(clean)
    declaration_comment["raft"] = declaration_comment["raft"].replace(
        declaration, f"// {declaration}", 1
    )
    with pytest.raises(SystemExit, match="plaintext limit must have one owner"):
        module._check_coordinator_limits(declaration_comment)

    expression = (
        "MAX_REPLICATED_COMMAND_PAYLOAD_BYTES "
        "+ MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES"
    )
    assert expression in clean["raft"]
    expression_comment = copy.deepcopy(clean)
    expression_comment["raft"] = expression_comment["raft"].replace(
        expression, "128 * 1024 * 1024 + 64", 1
    )
    expression_comment["raft"] += f"\n// {expression}\n"
    with pytest.raises(SystemExit, match="apply its named overhead"):
        module._check_coordinator_limits(expression_comment)

    expression_literal = copy.deepcopy(clean)
    expression_literal["raft"] = expression_literal["raft"].replace(
        expression, "128 * 1024 * 1024 + 64", 1
    )
    expression_literal["raft"] += f'\nconst SPOOF: &str = "{expression}";\n'
    with pytest.raises(SystemExit, match="apply its named overhead"):
        module._check_coordinator_limits(expression_literal)

    comment_only_duplicate = copy.deepcopy(clean)
    comment_only_duplicate["raft"] += f"\n// {declaration}\n// {expression}\n"
    module._check_coordinator_limits(comment_only_duplicate)


def test_native_store_source_union_contains_root_and_scope_binding_lanes() -> None:
    module = _gate_module()
    source = module.mutation_kernel_source()
    assert 'TableDefinition::new("mutation_store_root")' in source
    assert 'TableDefinition::new("mutation_scope_bindings")' in source
    assert "pub fn create_owner<D: OwnerDomain>(" in source
    assert "pub fn open_owner<D: OwnerDomain>(" in source
    assert "pub fn authenticate_scope<D: OwnerDomain>(" in source
    assert "pub fn bind_serving_scope<D: OwnerDomain>(" in source
    # MutationWrite -> AdmittedMutation (the write capability admit() returns).
    assert "pub struct AdmittedMutation<'a, D: OwnerDomain>" in source
    assert "MutationVersionScope" not in source


def test_product_schema_one_is_disjoint_from_prototype_tables() -> None:
    module = _gate_module()
    source = module.mutation_kernel_source()

    # Renamed STORAGE_KERNEL_SCHEMA_VERSION and bumped 1 -> 2 by b90e42a7
    # (the same commit that closed the storage/ledger key split); re-baselined
    # to the real current name/value.
    assert "pub const STORAGE_KERNEL_SCHEMA_VERSION: u16 = 2;" in source
    live_tables = (
        "mutation_store_root",
        "mutation_scope_bindings",
        "mutation_owner_manifest",
        "ledger_batches",
        "ledger_maintenance",
        "ledger_versions",
        "ledger_fences",
        "ledger_outbox",
        "mutation_outbox_topic_index",
        "ledger_private_payloads",
        "mutation_outbox_consumers",
        "mutation_outbox_deliveries",
        "mutation_outbox_cursors",
        "mutation_outbox_claim_cursors",
        "mutation_outbox_fairness",
        "mutation_replay_nonces",
        "mutation_replay_operations",
        "mutation_classes",
    )
    for name in live_tables:
        assert f'TableDefinition::new("{name}")' in source
    for stem in (
        "mutation_store_root",
        "mutation_scope_bindings",
        "mutation_batches",
        "mutation_idempotency",
        "mutation_versions",
        "mutation_fences",
        "mutation_outbox",
        "mutation_private_payloads",
    ):
        assert f'"{stem}_v3"' in source


def test_m1_scanner_rejects_digest_and_owner_write_regressions() -> None:
    module = _gate_module()
    contract, native_store, contract_tests, native_store_tests, row_delta = _m1_sources(
        module
    )

    with pytest.raises(SystemExit, match="versioned LP32 SHA-256"):
        module.check_m1_mutation_identity(
            contract.replace("eg/mutation-scope-identity/v1", "retired-digest", 1),
            native_store,
            contract_tests,
            native_store_tests,
            row_delta,
        )
    with pytest.raises(SystemExit, match="owner-minted write"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace(
                "pub(crate) fn binding_for_write(",
                "pub(crate) fn unchecked_binding(",
                1,
            ),
            contract_tests,
            native_store_tests,
            row_delta,
        )


def test_m1_scanner_rejects_unbounded_write_and_magic_byte_authenticity() -> None:
    module = _gate_module()
    contract, native_store, contract_tests, native_store_tests, row_delta = _m1_sources(
        module
    )

    with pytest.raises(SystemExit, match="preflight size/count budgets"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace("fn encode_bounded", "fn allocate_unbounded", 1),
            contract_tests,
            native_store_tests,
            row_delta,
        )
    with pytest.raises(SystemExit, match="canonical integrity authority"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace(
                "private recovery payload failed canonical authentication",
                "bytes[0] == 0xE6",
                1,
            ),
            contract_tests,
            native_store_tests,
            row_delta,
        )


def test_m1_scanner_rejects_arbitrary_physical_root_and_semantic_bypass() -> None:
    module = _gate_module()
    contract, native_store, contract_tests, native_store_tests, row_delta = _m1_sources(
        module
    )

    with pytest.raises(SystemExit, match="canonical database root"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace("metadata.ino()", "caller_root_id", 1),
            contract_tests,
            native_store_tests,
            row_delta,
        )
    with pytest.raises(SystemExit, match="semantic index writes must use"):
        module.check_m1_mutation_identity(
            contract.replace("SemanticIndex", "LegacyIndex"),
            native_store,
            contract_tests,
            native_store_tests,
            row_delta,
        )


def test_row_delta_authority_matches_producer_migration_and_validator() -> None:
    module = _gate_module()
    contract, _, _, _, row_delta = _m1_sources(module)

    module._check_m1_identity_contract(contract, row_delta)
    assert (
        'const ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-schema-sources";'
        in row_delta
    )
    assert (
        'const LEGACY_ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-v2";' in row_delta
    )
    assert "const ROW_DELTA_VERSION: u16 = 3;" in row_delta


@pytest.mark.parametrize(
    ("target", "before", "after", "failure"),
    (
        (
            "producer",
            'const ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-schema-sources";',
            'const ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-v1";',
            "producer constant/version",
        ),
        (
            "producer",
            "const ROW_DELTA_VERSION: u16 = 3;",
            "const ROW_DELTA_VERSION: u16 = 4;",
            "producer constant/version",
        ),
        (
            "validator",
            '"sha256" | "sha256-row-delta-v2" | "sha256-row-delta-schema-sources"',
            '"sha256" | "sha256-row-delta-v2" | "sha256-row-delta-v4"',
            "accept exactly sha256",
        ),
    ),
)
def test_row_delta_authority_mutations_fail_closed(
    target: str, before: str, after: str, failure: str
) -> None:
    module = _gate_module()
    contract, _, _, _, row_delta = _m1_sources(module)
    source = row_delta if target == "producer" else contract
    assert before in source
    mutated = source.replace(before, after, 1)

    with pytest.raises(SystemExit, match=failure):
        module._check_m1_identity_contract(
            contract if target == "producer" else mutated,
            mutated if target == "producer" else row_delta,
        )


@pytest.mark.parametrize(
    "stale",
    ("sha256-row-delta-v1", "sha256-row-delta-prototype"),
)
def test_row_delta_retired_identities_cannot_hide_in_production(stale: str) -> None:
    module = _gate_module()
    contract, _, _, _, row_delta = _m1_sources(module)

    with pytest.raises(SystemExit, match="retired row-delta identity"):
        module._check_m1_identity_contract(
            contract + f'\nconst STALE: &str = "{stale}";', row_delta
        )


def _blob_sources(module):
    blob_store = module.read_compiler_family("src/server/blob/store.rs")
    blob_shared = module.read_compiler_family(
        "crates/eg-storage/src/owner/blob_shared.rs"
    )
    return blob_store.production, blob_shared.production, blob_store.with_tests


def test_blob_result_contract_matches_shipped_atomic_kernel() -> None:
    module = _gate_module()
    blob_store, blob_shared, blob_store_tests = _blob_sources(module)

    # Must not raise: the shipped kernel binds the MutationBatch, the shared
    # CAS/refcount tables, and the restart/replay/GC proof.
    module._check_blob_result_contract(blob_store, blob_shared, blob_store_tests)


@pytest.mark.parametrize(
    ("target", "needle", "failure"),
    (
        ("blob_store", "self.commit_native_batch", "atomically bind CAS"),
        ("blob_store", "insert_chunk_if_absent", "atomically bind CAS"),
        ("blob_shared", "CAS_REFCOUNT", "atomically bind CAS"),
        ("blob_shared", "checked_sub", "atomically bind CAS"),
        (
            "blob_store_tests",
            "direct_ref_acquire_compensation_and_gc_are_restart_replay_safe",
            "restart/replay/GC proof is missing",
        ),
    ),
)
def test_blob_result_contract_rejects_missing_marker(
    target: str, needle: str, failure: str
) -> None:
    module = _gate_module()
    blob_store, blob_shared, blob_store_tests = _blob_sources(module)
    sources = {
        "blob_store": blob_store,
        "blob_shared": blob_shared,
        "blob_store_tests": blob_store_tests,
    }
    assert needle in sources[target]
    # Replace EVERY occurrence: some markers (e.g. a table-name constant) also
    # appear at unrelated call sites in the same file, and a single-occurrence
    # replace would leave the check's own `in` membership test still true.
    sources[target] = sources[target].replace(needle, "REMOVED_MARKER")

    with pytest.raises(SystemExit, match=failure):
        module._check_blob_result_contract(
            sources["blob_store"], sources["blob_shared"], sources["blob_store_tests"]
        )


def test_m1_production_markers_cannot_be_supplied_by_test_only_source() -> None:
    module = _gate_module()
    contract, native_store, contract_tests, native_store_tests, row_delta = _m1_sources(
        module
    )
    marker = "SemanticIndex"
    assert marker in contract and marker in contract_tests

    with pytest.raises(SystemExit, match="semantic index writes must use"):
        module.check_m1_mutation_identity(
            contract.replace(marker, "LegacyIndex"),
            native_store,
            contract_tests,
            native_store_tests,
            row_delta,
        )


def test_module_tree_follows_declared_production_children_and_excludes_tests(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    root = tmp_path / "root.rs"
    children = tmp_path / "root"
    children.mkdir()
    root.write_text(
        "\n".join(
            (
                "mod external;",
                '#[path = "alternate.rs"] mod custom_path;',
                '#[doc = "#[cfg(test)]"] mod doc_cfg_literal;',
                '#[doc = "#[path = \\"missing.rs\\"]"] mod doc_path_literal;',
                'const ATTRIBUTE_TEXT: &str = "#[cfg(test)] #[path = '
                '\\"missing.rs\\"]";',
                "mod string_literal_attributes;",
                "mod inline_production { fn inline_production_marker() {} }",
                'mod inline_include { include!("nested_include.rs"); }',
                "mod inline_virtual { mod nested; }",
                "#[cfg(test)] mod same_line_test;",
                "#[cfg(test)] mod inline_test {",
                "    fn production_assertion_only_in_test() {}",
                "}",
                "mod inner_test_inline {",
                "    #![cfg(test)]",
                "    fn inner_test_inline_marker() {}",
                "}",
                "mod inner_test_external;",
                "#[cfg(test)] fn test_only_function_marker() {}",
                '#[cfg(all(test, feature = "ship"))]',
                "mod test_conjunction { fn test_conjunction_marker() {} }",
                "#[cfg(not(test))]",
                "mod non_test { fn non_test_marker() {} }",
                '#[cfg(any(test, feature = "ship"))]',
                "mod mixed { fn mixed_marker() {} }",
                "mod mixed_order_first;",
                'include!(/* before */ "mixed_order_middle.rs" /* after */);',
                "mod mixed_order_last;",
                'include!(/* before */ "included.rs" /* after */);',
                '#[cfg(test)] include!("test_included.rs");',
                'const DECLARATION_TEXT: &str = r#"mod missing_raw_string;"#;',
                "// mod missing_line_comment;",
                "/* mod missing_block_comment; */",
                "",
            )
        ),
        encoding="utf-8",
    )
    (children / "external.rs").write_text(
        "fn external_production_marker() {}\n", encoding="utf-8"
    )
    (tmp_path / "alternate.rs").write_text(
        "fn custom_path_marker() {}\n", encoding="utf-8"
    )
    (children / "doc_cfg_literal.rs").write_text(
        "fn doc_cfg_literal_marker() {}\n", encoding="utf-8"
    )
    (children / "doc_path_literal.rs").write_text(
        "fn doc_path_literal_marker() {}\n", encoding="utf-8"
    )
    (children / "string_literal_attributes.rs").write_text(
        "fn string_literal_attributes_marker() {}\n", encoding="utf-8"
    )
    (children / "same_line_test.rs").write_text(
        "fn same_line_test_marker() {}\n", encoding="utf-8"
    )
    inline_virtual = children / "inline_virtual"
    inline_virtual.mkdir()
    (inline_virtual / "nested.rs").write_text(
        "fn inline_virtual_child_marker() {}\n", encoding="utf-8"
    )
    (children / "inner_test_external.rs").write_text(
        "#![cfg(test)]\nfn inner_test_external_marker() {}\n", encoding="utf-8"
    )
    (children / "mixed_order_first.rs").write_text(
        "fn mixed_order_first_marker() {}\n", encoding="utf-8"
    )
    (tmp_path / "mixed_order_middle.rs").write_text(
        "fn mixed_order_middle_marker() {}\n", encoding="utf-8"
    )
    (children / "mixed_order_last.rs").write_text(
        "fn mixed_order_last_marker() {}\n", encoding="utf-8"
    )
    (tmp_path / "included.rs").write_text(
        "fn included_production_marker() {}\n", encoding="utf-8"
    )
    (tmp_path / "nested_include.rs").write_text(
        "fn nested_include_marker() {}\n", encoding="utf-8"
    )
    (tmp_path / "test_included.rs").write_text(
        "fn test_included_marker() {}\n", encoding="utf-8"
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    for marker in (
        "external_production_marker",
        "custom_path_marker",
        "doc_cfg_literal_marker",
        "doc_path_literal_marker",
        "string_literal_attributes_marker",
        "inline_production_marker",
        "nested_include_marker",
        "inline_virtual_child_marker",
        "non_test_marker",
        "mixed_marker",
        "mixed_order_first_marker",
        "mixed_order_middle_marker",
        "mixed_order_last_marker",
        "included_production_marker",
    ):
        assert marker in source
    for marker in (
        "same_line_test_marker",
        "production_assertion_only_in_test",
        "test_only_function_marker",
        "test_conjunction_marker",
        "inner_test_inline_marker",
        "inner_test_external_marker",
        "test_included_marker",
    ):
        assert marker not in source
    with pytest.raises(SystemExit, match="production assertion missing"):
        module.require(
            "production_assertion_only_in_test" in source,
            "production assertion missing",
        )
    order = [
        source.index(marker)
        for marker in (
            "mixed_order_first_marker",
            "mixed_order_middle_marker",
            "mixed_order_last_marker",
        )
    ]
    assert order == sorted(order)

    source_with_tests = module.read_module_tree("root.rs", include_tests=True)
    assert "same_line_test_marker" in source_with_tests
    assert "production_assertion_only_in_test" in source_with_tests
    assert "test_only_function_marker" in source_with_tests
    assert "test_conjunction_marker" in source_with_tests
    assert "inner_test_inline_marker" in source_with_tests
    assert "inner_test_external_marker" in source_with_tests
    assert "test_included_marker" in source_with_tests


def test_module_tree_accepts_bound_macro_rules_attribute_templates(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        "macro_rules! emit_items {\n"
        "    ($( $(#[$attribute:meta])* $name:ident, )+) => {\n"
        "        $( $(#[$attribute])* fn $name() {} )+\n"
        "    };\n"
        "}\n"
        "fn production_after_macro_template() {}\n"
        "#[cfg(test)] fn test_only_after_macro_template() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    production = module.read_module_tree("root.rs")
    assert "production_after_macro_template" in production
    assert "test_only_after_macro_template" not in production
    with_tests = module.read_module_tree("root.rs", include_tests=True)
    assert "test_only_after_macro_template" in with_tests


@pytest.mark.parametrize(
    "source",
    (
        "#[$attribute] fn malformed() {}\n",
        "macro_rules! unbound { ($name:ident) => { "
        "#[$attribute] fn generated() {} }; }\n",
        "macro_rules! wrong_fragment { ($(#[$attribute:ident])*) => { "
        "$(#[$attribute])* fn generated() {} }; }\n",
        "macro_rules! fragment_in_output { () => { "
        "#[$attribute:meta] fn generated() {} }; }\n",
        "macro_rules! cross_arm { ($(#[$attribute:meta])*) => {}; "
        "() => { #[$attribute] fn generated() {} }; }\n",
        "macro_rules! cross_arm_comma { ($(#[$attribute:meta])*) => {}, "
        "() => { #[$attribute] fn generated() {} }; }\n",
        "macro_rules! late_binding { (#[$attribute] $attribute:meta) => {}; }\n",
        "macro_rules! early_binding { ($attribute:meta #[$attribute]) => {}; }\n",
        "macro_rules! duplicate_matcher_template { "
        "(#[$attribute:meta] #[$attribute]) => {}; }\n",
        "macro_rules_not! spoof { ($(#[$attribute:meta])*) => { "
        "$(#[$attribute])* fn generated() {} }; }\n",
        "#[123] fn malformed_real_attribute() {}\n",
    ),
)
def test_module_tree_rejects_unproven_macro_attribute_templates(
    tmp_path: Path, monkeypatch, source: str
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(source, encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(
        SystemExit, match="Rust attribute must start with an identifier"
    ):
        module.read_module_tree("root.rs")


def test_module_tree_skips_test_only_dynamic_include_before_path_parsing(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        '#[cfg(test)] include!(concat!("missing", ".rs"));\n'
        "fn production_after_test_include_marker() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    assert "production_after_test_include_marker" in source
    assert "missing" not in source
    with pytest.raises(SystemExit, match="one static Rust string literal"):
        module.read_module_tree("root.rs", include_tests=True)


def test_module_tree_fails_closed_on_conditional_path_attribute(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        '#[cfg_attr(feature = "alternate", path = "alternate.rs")]\n'
        "mod conditional_path;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="conditional Rust path attribute"):
        module.read_module_tree("root.rs")


def test_module_tree_applies_conditional_cfg_under_test_false(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        "#[cfg_attr(not(test), cfg(test))]\n"
        "fn cfg_attr_test_only_marker() {}\n"
        "fn production_after_cfg_attr_marker() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    assert "cfg_attr_test_only_marker" not in source
    assert "production_after_cfg_attr_marker" in source
    source_with_tests = module.read_module_tree("root.rs", include_tests=True)
    assert "cfg_attr_test_only_marker" in source_with_tests


def test_module_tree_excludes_complete_cfg_test_block_expression_item(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        "#[cfg(test)]\n"
        "const TEST_ONLY_BLOCK: &str = if true {\n"
        '    "first_test_branch"\n'
        "} else {\n"
        '    "trailing_block_test_only_marker"\n'
        "};\n"
        "fn production_after_block_item_marker() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    assert "trailing_block_test_only_marker" not in source
    assert "production_after_block_item_marker" in source
    source_with_tests = module.read_module_tree("root.rs", include_tests=True)
    assert "trailing_block_test_only_marker" in source_with_tests


def test_module_tree_excludes_complete_cfg_test_function_pointer_const(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        "#[cfg(test)] fn first() {}\n"
        "#[cfg(test)] fn trailing_callback_marker() {}\n"
        "#[cfg(test)]\n"
        "const CALLBACK: fn() = if true { first } else { trailing_callback_marker };\n"
        "fn production_after_callback_marker() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    assert "trailing_callback_marker" not in source
    assert "production_after_callback_marker" in source
    source_with_tests = module.read_module_tree("root.rs", include_tests=True)
    assert "trailing_callback_marker" in source_with_tests


def test_module_tree_combines_enclosing_and_nested_cfg_predicates(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    children.mkdir()
    (tmp_path / "root.rs").write_text(
        '#[cfg(feature = "ship")]\n'
        "mod inline_outer {\n"
        '    #[cfg(not(feature = "ship"))]\n'
        "    fn impossible_inline_marker() {}\n"
        "    fn reachable_inline_marker() {}\n"
        "}\n"
        '#[cfg(feature = "ship")]\n'
        "mod external_outer;\n",
        encoding="utf-8",
    )
    (children / "external_outer.rs").write_text(
        '#[cfg(not(feature = "ship"))]\n'
        "fn impossible_external_marker() {}\n"
        "fn reachable_external_marker() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    assert "impossible_inline_marker" not in source
    assert "impossible_external_marker" not in source
    assert "reachable_inline_marker" in source
    assert "reachable_external_marker" in source
    source_with_tests = module.read_module_tree("root.rs", include_tests=True)
    assert "impossible_inline_marker" in source_with_tests
    assert "impossible_external_marker" in source_with_tests


def test_module_tree_resolves_raw_and_uppercase_module_identifiers(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    inline_children = children / "Inline"
    inline_children.mkdir(parents=True)
    (tmp_path / "root.rs").write_text(
        "mod r#type;\n"
        "mod Foo;\n"
        "mod Inline {\n"
        "    mod r#match { fn inline_raw_marker() {} }\n"
        '    #[path = "upper_override.rs"] mod Bar;\n'
        "}\n"
        '#[path = "raw_override.rs"] mod r#async;\n',
        encoding="utf-8",
    )
    (children / "type.rs").write_text("fn raw_external_marker() {}\n", encoding="utf-8")
    (children / "Foo.rs").write_text(
        "fn uppercase_external_marker() {}\n", encoding="utf-8"
    )
    (inline_children / "upper_override.rs").write_text(
        "fn uppercase_path_marker() {}\n", encoding="utf-8"
    )
    (tmp_path / "raw_override.rs").write_text(
        "fn raw_path_marker() {}\n", encoding="utf-8"
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    for marker in (
        "raw_external_marker",
        "uppercase_external_marker",
        "inline_raw_marker",
        "uppercase_path_marker",
        "raw_path_marker",
    ):
        assert marker in source


def test_module_tree_fails_closed_on_unsupported_module_identifier(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text("mod Δ;\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="unsupported Rust module declaration"):
        module.read_module_tree("root.rs")


@pytest.mark.parametrize(
    "conditional",
    (
        "custom_attribute",
        "allow::custom",
        "deny::custom",
        "derive(CustomMacro)",
        "doc::custom",
        "forbid::custom",
        "recursion_limit::custom",
        "warn::custom",
        "allow(dead_code) trailing",
        "allow(dead_code)::custom",
        "doc(hidden) trailing",
        'doc = include_str!("missing.md")',
        'recursion_limit("256")',
        'recursion_limit = concat!("2", "56")',
        "allow()",
    ),
)
def test_module_tree_fails_closed_on_unknown_conditional_attribute(
    tmp_path: Path, monkeypatch, conditional: str
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        f'#[cfg_attr(feature = "ship", {conditional})]\nfn guarded() {{}}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="unsupported conditional Rust attribute"):
        module.read_module_tree("root.rs")


def test_module_tree_accepts_only_complete_inert_conditional_attribute_shapes(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        '#![cfg_attr(test, recursion_limit = "256")]\n'
        '#[cfg_attr(feature = "ship", allow(dead_code, unused_variables))]\n'
        # `CustomMacro` is not a real derive this scanner allows -- it fails
        # closed on unknown derive paths by design (`_CFG_ATTR_KNOWN_DERIVES`
        # in rust_module_tree.py). Use one of the actually-allowlisted paths
        # so this fixture exercises the known-inert shape it is named for.
        '#[cfg_attr(feature = "ship", derive(serde::Serialize))]\n'
        '#[cfg_attr(feature = "ship", warn(clippy::pedantic))]\n'
        '#[cfg_attr(feature = "ship", doc = "guarded item")]\n'
        "fn inert_attribute_marker() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read_module_tree("root.rs")
    assert "inert_attribute_marker" in source


def test_module_tree_fails_closed_on_declared_cycle(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(
        '#[path = "root.rs"] mod recursive;\n', encoding="utf-8"
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="cyclic Rust module declaration"):
        module.read_module_tree("root.rs")


def test_module_tree_fails_closed_on_ambiguous_declared_child(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    (children / "ambiguous").mkdir(parents=True)
    (tmp_path / "root.rs").write_text("mod ambiguous;\n", encoding="utf-8")
    (children / "ambiguous.rs").write_text("fn first() {}\n", encoding="utf-8")
    (children / "ambiguous" / "mod.rs").write_text("fn second() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="resolve to exactly one file"):
        module.read_module_tree("root.rs")


@pytest.mark.parametrize(
    ("root_source", "failure"),
    (
        ("mod missing;\n", "resolve to exactly one file"),
        ('include!("missing.rs");\n', "missing declared Rust module"),
    ),
)
def test_module_tree_fails_closed_on_missing_declared_child(
    tmp_path: Path, monkeypatch, root_source: str, failure: str
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(root_source, encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match=failure):
        module.read_module_tree("root.rs")


def test_compiler_family_supports_monolith_and_declared_split(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    (tmp_path / "monolith.rs").write_text(
        "fn monolith_production_marker() {}\n", encoding="utf-8"
    )
    split = tmp_path / "split"
    split.mkdir()
    (tmp_path / "split.rs").write_text(
        "mod production;\n#[cfg(test)] mod tests;\n", encoding="utf-8"
    )
    (split / "production.rs").write_text(
        "fn split_production_marker() {}\n", encoding="utf-8"
    )
    (split / "tests.rs").write_text(
        "fn split_test_only_marker() {}\n", encoding="utf-8"
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    monolith = module.read_compiler_family("monolith.rs")
    assert "monolith_production_marker" in monolith.production
    split_family = module.read_compiler_family("split.rs")
    assert "split_production_marker" in split_family.production
    assert "split_test_only_marker" not in split_family.production
    assert "split_test_only_marker" in split_family.with_tests


@pytest.mark.parametrize(
    "root_source",
    ("mod missing;\n", "#[cfg(test)] mod missing;\n"),
)
def test_compiler_family_fails_closed_on_missing_declared_children(
    tmp_path: Path, monkeypatch, root_source: str
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(root_source, encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="resolve to exactly one file"):
        module.read_compiler_family("root.rs")


def test_compiler_family_fails_closed_on_orphan_child(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    children.mkdir()
    (tmp_path / "root.rs").write_text("mod declared;\n", encoding="utf-8")
    (children / "declared.rs").write_text("fn declared() {}\n", encoding="utf-8")
    (children / "orphan.rs").write_text("fn orphan() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="orphan Rust module files"):
        module.read_compiler_family("root.rs")


def test_module_paths_returns_declared_closure_without_sibling_orphan_policy(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    children.mkdir()
    root = tmp_path / "root.rs"
    root.write_text("mod declared;\n", encoding="utf-8")
    declared = children / "declared.rs"
    declared.write_text("fn declared() {}\n", encoding="utf-8")
    orphan = children / "orphan.rs"
    orphan.write_text("fn orphan() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    paths = module.read_module_paths("root.rs")
    assert paths == {root.resolve(), declared.resolve()}
    assert orphan.resolve() not in paths


def test_module_paths_freezes_production_and_test_views(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    children.mkdir()
    root = tmp_path / "root.rs"
    root.write_text("mod live;\n#[cfg(test)] mod tests;\n", encoding="utf-8")
    live = children / "live.rs"
    live.write_text("fn live() {}\n", encoding="utf-8")
    tests = children / "tests.rs"
    tests.write_text("fn test_only() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    assert module.read_module_paths("root.rs", include_tests=False) == {
        root.resolve(),
        live.resolve(),
    }
    assert module.read_module_paths("root.rs") == {
        root.resolve(),
        live.resolve(),
        tests.resolve(),
    }


def test_module_paths_rejects_paths_outside_root(tmp_path: Path, monkeypatch) -> None:
    module = _gate_module()
    scanner_root = tmp_path / "scanner-root"
    scanner_root.mkdir()
    (scanner_root / "root.rs").write_text(
        '#[path = "../outside.rs"] mod outside;\n', encoding="utf-8"
    )
    (tmp_path / "outside.rs").write_text("fn outside() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", scanner_root)

    with pytest.raises(SystemExit, match="escapes root directory"):
        module.read_module_paths("root.rs")


@pytest.mark.parametrize(
    ("setup", "failure"),
    (
        ("missing", "resolve to exactly one file"),
        ("ambiguous", "resolve to exactly one file"),
        ("cycle", "cyclic Rust module declaration"),
    ),
)
def test_module_paths_fails_closed_on_invalid_closure(
    tmp_path: Path, monkeypatch, setup: str, failure: str
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    if setup == "missing":
        (tmp_path / "root.rs").write_text("mod child;\n", encoding="utf-8")
    elif setup == "ambiguous":
        (children / "child").mkdir(parents=True)
        (tmp_path / "root.rs").write_text("mod child;\n", encoding="utf-8")
        (children / "child.rs").write_text("fn first() {}\n", encoding="utf-8")
        (children / "child" / "mod.rs").write_text("fn second() {}\n", encoding="utf-8")
    else:
        (tmp_path / "root.rs").write_text(
            '#[path = "root.rs"] mod child;\n', encoding="utf-8"
        )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match=failure):
        module.read_module_paths("root.rs")


def test_module_paths_skips_cfg_impossible_production_child(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    children = tmp_path / "root"
    children.mkdir()
    root = tmp_path / "root.rs"
    root.write_text(
        '#[cfg(all(feature = "ship", not(feature = "ship")))] mod impossible;\n'
        "mod live;\n",
        encoding="utf-8",
    )
    live = children / "live.rs"
    live.write_text("fn live() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    assert module.read_module_paths("root.rs", include_tests=False) == {
        root.resolve(),
        live.resolve(),
    }


@pytest.mark.parametrize(
    ("root_source", "failure"),
    (
        (
            'fn active() { include!("hidden.rs"); }\n',
            "compiler-active Rust include was not consumed",
        ),
        (
            'fn active() { #[path = "hidden.rs"] mod hidden; }\n',
            "compiler-active Rust module declaration was not consumed",
        ),
    ),
)
def test_compiler_family_fails_closed_on_block_local_source_declarations(
    tmp_path: Path, monkeypatch, root_source: str, failure: str
) -> None:
    module = _gate_module()
    (tmp_path / "root.rs").write_text(root_source, encoding="utf-8")
    (tmp_path / "hidden.rs").write_text(
        "fn hidden_contract_marker() {}\n", encoding="utf-8"
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match=failure):
        module.read_compiler_family("root.rs")


@pytest.mark.parametrize(
    ("source", "opener", "closer"),
    [
        ('{ "}" }', "{", "}"),
        ("{ // }\n }", "{", "}"),
        ("{ /* outer /* nested */ still */ }", "{", "}"),
        (r"""{ '}' }""", "{", "}"),
        (r"""{ "escaped \" }" }""", "{", "}"),
        ("{'a}", "{", "}"),
    ],
)
def test_balanced_span_ignores_rust_comments_and_literals(
    source: str, opener: str, closer: str
) -> None:
    module = _gate_module()
    start = source.index(opener)

    assert module._balanced_span_from(source, start, opener, closer) == source.rindex(
        closer
    )


def test_balanced_span_rejects_unterminated_rust_block() -> None:
    module = _gate_module()

    with pytest.raises(SystemExit, match="unterminated balanced block"):
        module._balanced_span_from("{ /* unterminated", 0, "{", "}")


@pytest.mark.parametrize(
    ("source_name", "before", "after", "failure"),
    [
        (
            "consistency",
            "const MUTATION_APPLY_DURABLE_GRAPHREDB: &[&str] = &[\n"
            '    "AddEdge",\n    "AddEmbedding",\n    "AddNode",\n',
            "const MUTATION_APPLY_DURABLE_GRAPHREDB: &[&str] = &[\n"
            '    "AddEdge",\n    "AddEmbedding",\n',
            "live durable classifier and authoritative consistency inventory differ",
        ),
        (
            "mutation_runtime",
            '    "AddNode",\n',
            "",
            "gateway/native ownership does not exactly cover mutating policy",
        ),
        (
            "capabilities",
            '("AddNode", spec(make_policy(true, DurabilityDomain::GraphRedb',
            '("AddNode", spec(make_policy(true, DurabilityDomain::None',
            "mutates without a durability domain",
        ),
        (
            "graph_pipeline",
            "handlers::graph_ops::try_handle_gateway(",
            "handlers::graph_ops::removed_gateway(",
            "dispatch no longer routes graph/query/RDF gateways before the terminal "
            "handler",
        ),
        (
            "mutation_runtime",
            "crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT,",
            "crate::server::sparql_http::REMOVED_SPARQL_HTTP_UPDATE_EVENT,",
            "coordinated ApplyMutation event inventory differs",
        ),
        (
            "mutation_runtime",
            "if is_sparql_http_update(method) {",
            "if removed_sparql_http_update(method) {",
            "SPARQL ApplyMutation event is not routed through consensus fanout",
        ),
        (
            "mutation_runtime_tests",
            "fn clustered_mutation_inventory_is_complete()",
            "fn removed_clustered_mutation_inventory_proof()",
            "missing Rust function inventory: clustered_mutation_inventory_is_complete",
        ),
    ],
)
def test_inventory_drift_fails_closed(
    source_name: str,
    before: str,
    after: str,
    failure: str,
) -> None:
    module = _gate_module()
    sources = copy.deepcopy(module.mutation_inventory_sources())
    assert before in sources[source_name]
    sources[source_name] = sources[source_name].replace(before, after, 1)

    with pytest.raises(SystemExit, match=failure):
        module.check_mutation_inventory(sources)


@pytest.mark.parametrize(
    ("source_name", "injected", "failure"),
    [
        (
            "sparql_http",
            "\nfn bypass(core: &GraphCore) { core.mark_dirty(); }\n",
            "SPARQL HTTP",
        ),
        (
            "ros2_bridge",
            "\nfn bypass(core: &GraphCore, method: &Method) { "
            "crate::mutation_apply::apply(core, method); }\n",
            "ROS2 carrier",
        ),
    ],
)
def test_served_carrier_bypass_fails_closed(
    source_name: str,
    injected: str,
    failure: str,
) -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    sources[source_name] = injected + sources[source_name]

    with pytest.raises(SystemExit, match=failure):
        module.check_served_carrier_mutations(sources)


@pytest.mark.parametrize(
    ("source_name", "injected"),
    [
        ("cargo", '\ndataset-handle = ["server"]\n'),
        ("main", '\nconst EPISTEMIC_GRAPH_DATASET_ADDR: &str = "retired";\n'),
        ("state", "\nstruct ReturnedSurface { dataset_addr: String }\n"),
        ("server", '\nconst ROUTE: &str = "/dataset/export";\n'),
        ("dispatch", "\nfn coordinated_dataset_result_commit() {}\n"),
    ],
)
def test_retired_duplicate_dataset_surface_fails_closed(
    source_name: str,
    injected: str,
) -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    sources[source_name] += injected

    with pytest.raises(SystemExit, match="retired duplicate dataset"):
        module.check_served_carrier_mutations(sources)


def test_external_compute_contract_cannot_lose_native_result_stream() -> None:
    module = _gate_module()
    sources = module.mutation_inventory_sources()
    sources["external_compute_e2e"] = sources["external_compute_e2e"].replace(
        "KnowledgeStreamQuery::Job",
        "RemovedKnowledgeStreamJobQuery",
    )

    with pytest.raises(SystemExit, match="external-compute proof is missing"):
        module.check_served_carrier_mutations(sources)
