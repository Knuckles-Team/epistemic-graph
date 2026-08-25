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


def test_live_mutation_inventory_contract() -> None:
    module = _gate_module()
    module.check_mutation_inventory(module.mutation_inventory_sources())


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
            '("AddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb',
            '("AddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None',
            "a mutating capability has DurabilityDomain::None",
        ),
        (
            "dispatch",
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
