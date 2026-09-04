"""CI entry point for the current-only persisted mutation contract gate."""

from __future__ import annotations

import copy
import importlib.util
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


def test_persisted_mutation_contract_gate() -> None:
    _gate_module().main()


def test_persisted_contract_follows_mutation_batch_facade_tree() -> None:
    module = _gate_module()
    source = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")

    assert "pub struct MutationOperation" in source
    assert "pub domain: MutationDomain" in source


def test_mutation_receipts_bind_typed_scope_identity() -> None:
    module = _gate_module()
    contract = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")
    native_store = module.read_module_tree(
        "crates/eg-mutation-store/src/lib.rs", include_tests=True
    )

    module.check_m1_mutation_identity(contract, native_store)
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


def test_m1_freezes_exact_unmigrated_semantic_ship_blocker() -> None:
    module = _gate_module()
    paths = {path for path, _ in module._M1_UNMIGRATED_SEMANTIC_MARKERS}

    assert paths == {
        "crates/eg-query/src/tables/semantic_authority/build.rs",
        "crates/eg-query/src/tables/semantic_vectors.rs",
        "crates/eg-query/src/tables/store_embedding_tables.rs",
        "crates/eg-query/src/tables/store_semantic_records.rs",
        "src/server/semantic_index/coordinator.rs",
        "src/server/semantic_index/cas_purge.rs",
    }
    module._check_m1_unmigrated_semantic_inventory()


def test_m1_semantic_inventory_rejects_partial_migration(monkeypatch) -> None:
    module = _gate_module()
    original = module.read

    def partial(path: str) -> str:
        source = original(path)
        if path == "src/server/semantic_index/coordinator.rs":
            return source + "\neg_mutation_store::begin_partial();\n"
        return source

    monkeypatch.setattr(module, "read", partial)
    with pytest.raises(SystemExit, match="partial semantic mutation-ledger migration"):
        module._check_m1_unmigrated_semantic_inventory()


def test_live_mutation_inventory_contract() -> None:
    module = _gate_module()
    module.check_mutation_inventory(module.mutation_inventory_sources())


def _mutation_batch_facade(*modules: str) -> str:
    declarations = []
    for path in modules:
        name = Path(path).stem
        if name == "tests":
            declarations.append("#[cfg(test)]\nmod tests;")
        else:
            declarations.append(
                f'#[path = "mutation_batch/{name}.rs"]\nmod {name};'
            )
    return "\n".join(declarations)


def test_mutation_batch_source_union_contains_ordered_decomposed_lanes() -> None:
    module = _gate_module()
    source = module.mutation_inventory_sources()["mutation_batch"]
    markers = (
        "fn lower_canonical_operation(",
        "fn commit_internal_graph_methods(",
        "fn compile_methods(",
        "fn opaque_state_operation(",
    )
    positions = [source.index(marker) for marker in markers]
    assert positions == sorted(positions)


def test_durable_apply_source_union_contains_recursive_lanes() -> None:
    module = _gate_module()
    source = module.mutation_inventory_sources()["durable_apply"]
    for marker in (
        "pub(super) fn apply_nodes(",
        "pub(super) fn apply_edges(",
        "pub(super) fn apply_summary(",
        "pub(super) fn apply_scene(",
        "pub(super) fn apply_exchange(",
        "pub(super) fn apply_idempotency(",
    ):
        assert marker in source


def test_native_store_source_union_contains_root_and_scope_binding_lanes() -> None:
    module = _gate_module()
    source = module.read_module_tree("crates/eg-mutation-store/src/lib.rs")
    assert 'TableDefinition::new("mutation_store_root_v1")' in source
    assert 'TableDefinition::new("mutation_scope_bindings_v1")' in source
    assert "pub fn initialize<F>(" in source
    assert "pub fn bind_scope<F>(" in source
    assert "pub struct MutationWrite" in source
    assert "MutationVersionScope" not in source


def test_product_schema_one_is_disjoint_from_prototype_tables() -> None:
    module = _gate_module()
    source = module.read_module_tree("crates/eg-mutation-store/src/lib.rs")

    assert "pub const MUTATION_STORE_SCHEMA_VERSION: u16 = 1;" in source
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
        assert f'TableDefinition::new("{stem}_v1")' in source
        assert f'"{stem}_v3"' in source


def test_m1_scanner_rejects_digest_and_owner_write_regressions() -> None:
    module = _gate_module()
    contract = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")
    native_store = module.read_module_tree(
        "crates/eg-mutation-store/src/lib.rs", include_tests=True
    )

    with pytest.raises(SystemExit, match="versioned LP32 SHA-256"):
        module.check_m1_mutation_identity(
            contract.replace("eg/mutation-scope-identity/v1", "retired-digest", 1),
            native_store,
        )
    with pytest.raises(SystemExit, match="owner-minted write"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace("binding_for_write(write", "unchecked_binding("),
        )


def test_m1_scanner_rejects_unbounded_write_and_magic_byte_authenticity() -> None:
    module = _gate_module()
    contract = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")
    native_store = module.read_module_tree(
        "crates/eg-mutation-store/src/lib.rs", include_tests=True
    )

    with pytest.raises(SystemExit, match="preflight size/count budgets"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace("fn encode_bounded", "fn allocate_unbounded", 1),
        )
    with pytest.raises(SystemExit, match="canonical integrity authority"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace(
                "private recovery payload failed canonical authentication",
                "bytes[0] == 0xE6",
                1,
            ),
        )


def test_m1_scanner_rejects_arbitrary_physical_root_and_semantic_bypass() -> None:
    module = _gate_module()
    contract = module.read_module_tree("crates/eg-types/src/mutation_batch.rs")
    native_store = module.read_module_tree(
        "crates/eg-mutation-store/src/lib.rs", include_tests=True
    )

    with pytest.raises(SystemExit, match="canonical database root"):
        module.check_m1_mutation_identity(
            contract,
            native_store.replace("metadata.ino()", "caller_root_id", 1),
        )
    with pytest.raises(SystemExit, match="must remain gated"):
        module.check_m1_mutation_identity(
            contract.replace("semantic index mutations remain unserved", "semantic served", 1),
            native_store,
        )


@pytest.mark.parametrize(
    ("modules", "failure"),
    [
        (
            (
                "src/server/mutation_batch/canonical.rs",
                "src/server/mutation_batch/commit.rs",
                "src/server/mutation_batch/compile.rs",
                "src/server/mutation_batch/digest.rs",
            ),
            "mutation-batch module manifest drift",
        ),
        (
            (
                "src/server/mutation_batch/canonical.rs",
                "src/server/mutation_batch/commit.rs",
                "src/server/mutation_batch/commit.rs",
                "src/server/mutation_batch/digest.rs",
                "src/server/mutation_batch/tests.rs",
            ),
            "duplicate module declaration",
        ),
        (
            (
                "src/server/mutation_batch/commit.rs",
                "src/server/mutation_batch/canonical.rs",
                "src/server/mutation_batch/compile.rs",
                "src/server/mutation_batch/digest.rs",
                "src/server/mutation_batch/tests.rs",
            ),
            "mutation-batch module manifest drift",
        ),
    ],
)
def test_mutation_batch_module_manifest_rejects_omission_duplicate_and_reorder(
    modules: tuple[str, ...], failure: str
) -> None:
    module = _gate_module()
    source = _mutation_batch_facade(*(Path(path).stem for path in modules))
    with pytest.raises(SystemExit, match=failure):
        module._module_manifest("src/server/mutation_batch.rs", source)


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
            '("AddNode", make_policy(true, DurabilityDomain::GraphRedb',
            '("AddNode", make_policy(true, DurabilityDomain::None',
            "a mutating capability has DurabilityDomain::None",
        ),
        (
            "graph_pipeline",
            "handlers::graph_ops::try_handle_gateway(",
            "handlers::graph_ops::removed_gateway(",
            "dispatch no longer routes graph/query/RDF gateways before the terminal handler",
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
            "\nfn bypass(core: &GraphCore, method: &Method) { crate::mutation_apply::apply(core, method); }\n",
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
    marker = "#[cfg(test)]"
    assert marker in sources[source_name]
    sources[source_name] = sources[source_name].replace(marker, injected + marker, 1)

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
