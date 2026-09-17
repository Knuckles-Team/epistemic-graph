"""Static contract tests for the G-37 exact-binary certification harness."""

from __future__ import annotations

import copy
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "certify_exact_performance.py"
pytestmark = pytest.mark.exact_artifact


def _load_harness():
    spec = importlib.util.spec_from_file_location("certify_exact_performance", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _token(namespace: str, digit: str) -> str:
    return f"eg:{namespace}:{digit * 64}"


def _authority_document() -> dict[str, Any]:
    certifier = _token("certifier", "1")
    return {
        "schema_version": "1",
        "auth_secret": "a" * 64,
        "signer_id": certifier,
        "signer_key": "b" * 64,
        "context": {
            "principal": certifier,
            "tenant": _token("tenant", "2"),
            "audience": _token("audience", "3"),
            "agent_id": certifier,
            "roles": ["certifier"],
            "scopes": ["kg:admin"],
            "policy_version": _token("policy", "4"),
            "delegation": [],
        },
    }


def test_committed_workload_and_threshold_manifests_are_complete() -> None:
    harness = _load_harness()
    dataset, dataset_digest = harness._load_dataset(harness.DEFAULT_DATASET)
    thresholds, threshold_digest = harness._load_thresholds(harness.DEFAULT_THRESHOLDS)
    workload = harness._workload_from_manifest(dataset)

    assert workload.digest == dataset["expected_workload_sha256"]
    assert len(workload.node_operations) == dataset["node_count"]
    assert len(workload.edge_operations) == dataset["edge_count"]
    assert workload.route_partition_ref.startswith("eg:partition:")
    assert set(workload.modality_sources) == set(harness.MODALITIES)
    assert all(
        len(workload.modality_sources[modality]) == dataset["modality_records_per_kind"]
        for modality in harness.MODALITIES
    )
    assert set(thresholds["metrics"]) == set(harness.METRIC_CONTRACT)
    assert set(thresholds["complexity"]) == set(harness.COMPLEXITY_CONTRACT)
    assert len(dataset_digest) == 64
    assert len(threshold_digest) == 64


def test_authority_is_private_opaque_and_least_ambiguous(tmp_path: Path) -> None:
    harness = _load_harness()
    config = tmp_path / "authority.json"
    config.write_text(json.dumps(_authority_document()), encoding="utf-8")
    config.chmod(0o600)

    authority = harness._load_authority(config)
    assert authority.context["scopes"] == ["kg:admin"]
    assert authority.bootstrap_context["scopes"] == ["security:bootstrap"]
    assert authority.bootstrap_context["roles"] == []
    assert len(authority.fingerprint) == 64

    config.chmod(0o644)
    with pytest.raises(harness.CertificationError, match="authority_file_permissions"):
        harness._load_authority(config)


def test_gate_fails_closed_when_any_measurement_is_missing() -> None:
    harness = _load_harness()
    thresholds, _ = harness._load_thresholds(harness.DEFAULT_THRESHOLDS)

    results, failures = harness._evaluate(
        {}, thresholds["metrics"], harness.METRIC_CONTRACT
    )

    assert results == {}
    assert len(failures) == len(harness.METRIC_CONTRACT)
    assert all(value.startswith("missing:") for value in failures)


def test_measurement_metrics_preserve_operation_counters() -> None:
    harness = _load_harness()
    modalities = list(harness.MODALITIES)
    series = harness.MeasurementSeries(
        routing_groups=[[1.0, 1.0], [2.0, 2.0]],
        point_query_groups=[[1.0, 1.0], [2.0, 2.0]],
        node_ingest_groups=[[1.0], [2.0]],
        ingest_latencies=[1.0, 2.0],
        routing_latencies=[1.0, 2.0],
        point_query_latencies=[1.0, 2.0],
        analytics_latencies=[1.0, 2.0],
        job_submit_latencies=[1.0, 2.0],
        job_completion_latencies=[1.0, 2.0],
        modality_capability_latencies=[1.0, 2.0],
        modality_ingest_latencies=[1.0, 2.0],
        modality_query_latencies=[1.0, 2.0],
        modality_ingest_by_kind={modality: [1.0, 2.0] for modality in modalities},
        modality_query_groups_by_kind={
            modality: [[1.0, 2.0], [2.0, 3.0]] for modality in modalities
        },
        ingested_ops=6,
        query_rows=10,
        query_elapsed_ms=5.0,
    )
    memory = harness.MemoryMeasurements(
        all_samples=[10, 11],
        workload_samples=[11],
        peak_rss_kib=11,
        workload_peak_rss_kib=11,
    )

    metrics = harness._build_metrics(1.0, 10, series, memory)

    assert set(metrics) == set(harness.METRIC_CONTRACT)
    assert metrics["ingest_throughput_ops_per_second"] == 2_000.0
    assert metrics["point_query_throughput_rows_per_second"] == 2_000.0


def test_gate_fails_closed_when_any_workload_area_is_missing() -> None:
    harness = _load_harness()

    failures = harness._coverage_failures({})

    assert len(failures) == len(harness.COVERAGE_CONTRACT)
    assert all(value.startswith("missing_coverage:") for value in failures)


def test_gate_rejects_document_only_modality_coverage() -> None:
    harness = _load_harness()
    coverage: dict[str, dict[str, Any]] = {
        name: {"covered": True}
        for name in harness.COVERAGE_CONTRACT
        if name != "modality"
    }
    coverage["modality"] = {
        "component_probes": list(harness.MODALITIES),
        "ingests_by_modality": {"document": 8},
        "native_query_samples_by_modality": {"document": 24},
        "index_growth_ratio_by_modality": {"document": 1.0},
        "results_verified": True,
    }

    assert "invalid_coverage:modality_inventory" in harness._coverage_failures(coverage)


def test_evidence_rejects_paths_endpoints_and_secrets() -> None:
    harness = _load_harness()
    document = _authority_document()
    authority = harness.AuthorityConfig(
        document["auth_secret"],
        document["signer_id"],
        document["signer_key"],
        document["context"],
    )

    with pytest.raises(harness.CertificationError, match="local_reference"):
        harness._assert_evidence_safe({"value": "/mnt/example/private"}, authority)
    with pytest.raises(harness.CertificationError, match="secret"):
        harness._assert_evidence_safe({"value": authority.auth_secret}, authority)


def test_all_modality_fixtures_contain_only_opaque_governance() -> None:
    harness = _load_harness()
    authority = {
        "tenant_ref": _token("tenant", "2"),
        "access_policy_ref": _token("access-policy", "3"),
        "purpose_ref": _token("purpose", "4"),
        "maximum_classification": "restricted",
    }
    sources = {
        "document": b"alpha beta gamma",
        "image": harness.PNG_FIXTURE,
        "audio": harness._wav_fixture(),
        "video": harness._mp4_fixture(),
    }

    for index, modality in enumerate(harness.MODALITIES, start=1):
        source = sources[modality]
        bundle, occurrence, idempotency = harness._modality_bundle(
            authority, modality, source, 37, index
        )

        assert occurrence.startswith("eg:occurrence:")
        assert idempotency.startswith("eg:idempotency:")
        assert bundle["artifacts"][0]["modality"] == modality
        assert bundle["renditions"][0]["modality"] == modality
        assert bundle["privacy"]["raw_pii_persisted"] is False
        assert bundle["privacy"]["local_identifiers_persisted"] is False
        assert source not in harness.msgpack.packb(bundle, use_bin_type=True)


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _probe_result(
    scenario: dict[str, Any], *, constant_latency: bool = False
) -> dict[str, Any]:
    rows = []
    for row_ordinal, row in enumerate(scenario["rows"], start=1):
        scales = []
        for scale_ordinal, scale in enumerate(scenario["scales"], start=1):
            samples = [
                1_000
                if constant_latency
                else 1_000 + row_ordinal * 100 + scale_ordinal * 10 + repetition
                for repetition in range(scenario["repetitions"])
            ]
            scales.append(
                {
                    "scale": scale,
                    "work_units": 1,
                    "memory_bytes": 1,
                    "latency_ns": samples,
                }
            )
        rows.append(
            {
                "row_id": row["row_id"],
                "scales": scales,
                "equivalence": {check: True for check in row["equivalence_checks"]},
            }
        )
    return {
        "schema_version": "1",
        "protocol": "g37.performance-probe.v1",
        "scenario_id": scenario["scenario_id"],
        "driver": scenario["driver"],
        "rows": rows,
    }


def test_scenario_manifest_covers_every_ledger_row_exactly_once() -> None:
    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    scenarios = contracts.manifest["scenarios"]
    rows = [row["row_id"] for scenario in scenarios for row in scenario["rows"]]

    assert len(scenarios) == harness.EXPECTED_SCENARIO_COUNT == 30
    assert len(rows) == harness.EXPECTED_LEDGER_ROW_COUNT == 54
    assert len(set(rows)) == len(rows)
    assert set(rows) == set(contracts.ledger_rows)
    assert list(contracts.ledger_rows) == [
        f"G37-HP-{ordinal:03d}" for ordinal in range(1, 55)
    ]
    assert len(contracts.manifest_sha256) == 64
    assert len(contracts.schema_sha256) == 64
    assert len(contracts.ledger_sha256) == 64


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unknown"])
def test_scenario_manifest_rejects_row_coverage_mutations(
    tmp_path: Path, mutation: str
) -> None:
    harness = _load_harness()
    manifest = copy.deepcopy(
        json.loads(harness.DEFAULT_SCENARIOS.read_text(encoding="utf-8"))
    )
    if mutation == "missing":
        manifest["scenarios"][0]["rows"].pop()
    elif mutation == "duplicate":
        manifest["scenarios"][1]["rows"][0]["row_id"] = "G37-HP-001"
    else:
        manifest["scenarios"][1]["rows"][0]["row_id"] = "G37-HP-999"
    path = tmp_path / "scenarios.json"
    _write_manifest(path, manifest)

    with pytest.raises(
        harness.CertificationError, match="invalid_scenario_row_coverage"
    ):
        harness._load_scenario_contracts(path)


def test_probe_result_rejects_missing_unknown_duplicate_and_constant_evidence() -> None:
    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    scenario = contracts.manifest["scenarios"][0]

    with pytest.raises(
        harness.CertificationError, match="constant_scenario_probe_evidence"
    ):
        harness._validate_scenario_probe_result(
            _probe_result(scenario, constant_latency=True), scenario
        )

    missing = _probe_result(scenario)
    missing["rows"].pop()
    with pytest.raises(
        harness.CertificationError, match="invalid_scenario_probe_row_coverage"
    ):
        harness._validate_scenario_probe_result(missing, scenario)

    unknown = _probe_result(scenario)
    unknown["rows"][0]["row_id"] = "G37-HP-999"
    with pytest.raises(
        harness.CertificationError, match="invalid_scenario_probe_row_coverage"
    ):
        harness._validate_scenario_probe_result(unknown, scenario)

    duplicate = _probe_result(scenario)
    duplicate["rows"][1]["row_id"] = duplicate["rows"][0]["row_id"]
    with pytest.raises(
        harness.CertificationError, match="invalid_scenario_probe_row_coverage"
    ):
        harness._validate_scenario_probe_result(duplicate, scenario)


def test_evaluator_emits_digest_bound_evidence_for_all_54_rows() -> None:
    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    executions = {}
    for scenario in contracts.manifest["scenarios"]:
        result = harness._validate_scenario_probe_result(
            _probe_result(scenario), scenario
        )
        executions[scenario["scenario_id"]] = harness.ScenarioExecution(
            result=result,
            elapsed_ms=1.0,
            peak_rss_bytes=1,
            rss_samples=1,
        )
    binding = "c" * 64

    rows, families, failures = harness._evaluate_scenarios(
        contracts, executions, binding
    )

    assert failures == []
    assert len(rows) == 54
    assert len(families) == 30
    assert all(
        evidence["evidence_binding_sha256"] == binding for evidence in rows.values()
    )
    assert all(evidence["passed"] is True for evidence in rows.values())
    assert all(
        set(evidence["threshold_results"])
        == {
            "work_units",
            "work_growth_ratio",
            "peak_memory_bytes",
            "memory_growth_ratio",
            "latency_p99_ms",
            "latency_growth_ratio",
        }
        for evidence in rows.values()
    )


@pytest.fixture(scope="module")
def compiled_probe_sources() -> tuple[str, dict[str, str]]:
    probe_source = (ROOT / "src" / "performance_probe.rs").read_text(encoding="utf-8")
    probe_modules = re.findall(r"^mod ([a-z_]+);$", probe_source, flags=re.MULTILINE)
    assert "contract" in probe_modules
    module_sources = {
        name: (ROOT / "src" / "performance_probe" / f"{name}.rs").read_text(
            encoding="utf-8"
        )
        for name in probe_modules
    }
    return probe_source, module_sources


def test_exact_binary_probe_entrypoint_uses_declared_contract_owner(
    compiled_probe_sources: tuple[str, dict[str, str]],
) -> None:
    probe_source, _module_sources = compiled_probe_sources
    main_source = (ROOT / "src" / "main.rs").read_text(encoding="utf-8")
    assert "mod performance_probe;" in main_source
    assert "exact_performance_probe" in main_source
    assert "performance_probe::run_stdio(root)?" in main_source
    assert "contract::scenario_contract(&request.scenario_id)" in probe_source
    assert "contract::probe_row(" in probe_source


def _table(contract_source: str, name: str, element: str) -> list[str]:
    """The bodies of every `<element> { .. }` literal in `static <name>`."""

    header = f"static {name}: &[{element}] = &["
    assert contract_source.count(header) == 1, f"{name} table not found"
    table = contract_source.split(header, 1)[1].split("\n];", 1)[0]
    return re.findall(rf"\b{element} \{{(.*?)\n    \}},", table, flags=re.DOTALL)


_RUNNER = re.compile(
    r"\brunner: ProbeRunner::([A-Z][A-Za-z]+)"
    r"\(super::([a-z_]+)::([a-z_0-9]+)\)"
)


def _function_body(source: str, header: str) -> str:
    """The text of the item that starts at `header`, up to its closing brace."""

    return source.split(header, 1)[1].split("\n}", 1)[0]


def _string_list(body: str, field: str) -> list[str]:
    match = re.search(rf"\b{field}: &\[(.*?)\]", body, flags=re.DOTALL)
    assert match is not None, f"no `{field}` list in {body!r}"
    return re.findall(r'"([^"]+)"', match.group(1))


def _string_field(body: str, field: str) -> str:
    match = re.search(rf'\b{field}: "([^"]+)"', body)
    assert match is not None, f"no `{field}` string in {body!r}"
    return match.group(1)


def _check_scenario_table(contract_source: str, scenarios: list[dict]) -> None:
    """SCENARIO_CONTRACTS lists the manifest's scenarios, in order, exactly."""

    table = [
        (
            _string_field(body, "id"),
            _string_field(body, "driver"),
            _string_list(body, "rows"),
        )
        for body in _table(contract_source, "SCENARIO_CONTRACTS", "ScenarioContract")
    ]
    assert table == [
        (
            scenario["scenario_id"],
            scenario["driver"],
            [row["row_id"] for row in scenario["rows"]],
        )
        for scenario in scenarios
    ]
    lookup = _function_body(contract_source, "fn scenario_contract")
    assert "SCENARIO_CONTRACTS" in lookup
    assert "contract.id == scenario_id" in lookup


def _check_equivalence_table(contract_source: str, contracts: Any) -> None:
    """Every ledger row has exactly one contract, carrying its manifest checks."""

    rows = {}
    for body in _table(contract_source, "ROW_CONTRACTS", "RowContract"):
        row_id = _string_field(body, "id")
        assert row_id not in rows, f"{row_id} has two contracts"
        rows[row_id] = _string_list(body, "equivalence")
    assert set(rows) == set(contracts.ledger_rows)
    for scenario in contracts.manifest["scenarios"]:
        for row in scenario["rows"]:
            assert rows[row["row_id"]] == row["equivalence_checks"], row["row_id"]
    lookup = _function_body(contract_source, "fn row_equivalence_contract")
    assert "row_contract(row_id)" in lookup
    assert "contract.equivalence" in lookup


def _check_dispatch_table(
    contract_source: str, module_sources: dict[str, str], ledger_rows: Any
) -> None:
    """Every ledger row dispatches, through `probe_row`, to a real probe function."""

    runner_enum = contract_source.split("enum ProbeRunner {", 1)[1].split("\n}", 1)[0]
    variants = set(re.findall(r"^\s{4}([A-Z][A-Za-z]+)\(", runner_enum, flags=re.M))
    run_arms = contract_source.split("impl ProbeRunner {", 1)[1].split("\n}", 1)[0]
    assert variants == set(re.findall(r"Self::([A-Z][A-Za-z]+)\(run\) =>", run_arms))

    dispatched = []
    for body in _table(contract_source, "ROW_CONTRACTS", "RowContract"):
        runner = re.search(_RUNNER, body)
        assert runner is not None, f"no runner in {body!r}"
        variant, module, function = runner.groups()
        assert variant in variants
        assert re.search(rf"\bfn {function}\(", module_sources[module]), (
            f"{module}::{function} is not defined"
        )
        dispatched.append(_string_field(body, "id"))
    assert len(dispatched) == len(set(dispatched)) == 54
    assert set(dispatched) == set(ledger_rows)

    probe_row = _function_body(contract_source, "pub(super) fn probe_row(")
    assert "row_contract(row_id)" in probe_row
    assert (
        ".runner" in probe_row
        and ".run(row_id, scale, seed, repetition, probe_root)" in probe_row
    )


def test_exact_binary_probe_source_covers_manifest_inventory(
    compiled_probe_sources: tuple[str, dict[str, str]],
) -> None:
    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    _probe_source, module_sources = compiled_probe_sources
    _check_scenario_table(module_sources["contract"], contracts.manifest["scenarios"])


def test_exact_binary_probe_equivalence_checks_match_manifest(
    compiled_probe_sources: tuple[str, dict[str, str]],
) -> None:
    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    _probe_source, module_sources = compiled_probe_sources
    _check_equivalence_table(module_sources["contract"], contracts)


def test_exact_binary_probe_contract_checks_refuse_a_mismapped_row(
    compiled_probe_sources: tuple[str, dict[str, str]],
) -> None:
    """Each contract check fails when one row is mapped to the wrong place."""

    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    _probe_source, module_sources = compiled_probe_sources
    source = module_sources["contract"]

    moved_row = source.replace(
        'rows: &["G37-HP-006", "G37-HP-007"]', 'rows: &["G37-HP-007", "G37-HP-006"]', 1
    )
    assert moved_row != source
    with pytest.raises(AssertionError):
        _check_scenario_table(moved_row, contracts.manifest["scenarios"])

    wrong_checks = source.replace(
        'equivalence: &["cursor_page_matches_reference"]',
        'equivalence: &["cdc_suffix_matches_reference"]',
        1,
    )
    assert wrong_checks != source
    with pytest.raises(AssertionError):
        _check_equivalence_table(wrong_checks, contracts)

    missing_probe = source.replace(
        "ProbeRunner::Scale(super::storage::probe_tms)",
        "ProbeRunner::Scale(super::storage::probe_missing)",
        1,
    )
    assert missing_probe != source
    with pytest.raises(AssertionError):
        _check_dispatch_table(missing_probe, module_sources, contracts.ledger_rows)

    duplicated_row = source.replace('id: "G37-HP-054"', 'id: "G37-HP-053"', 1)
    assert duplicated_row != source
    with pytest.raises(AssertionError):
        _check_dispatch_table(duplicated_row, module_sources, contracts.ledger_rows)


def test_exact_binary_probe_dispatch_covers_manifest_inventory(
    compiled_probe_sources: tuple[str, dict[str, str]],
) -> None:
    harness = _load_harness()
    contracts = harness._load_scenario_contracts(harness.DEFAULT_SCENARIOS)
    probe_source, module_sources = compiled_probe_sources
    _check_dispatch_table(
        module_sources["contract"], module_sources, contracts.ledger_rows
    )
    compiled_probe_source = "\n".join([probe_source, *module_sources.values()])
    for required_real_surface in (
        "ServedModalityRuntime",
        "JobStore",
        "TruthMaintenance",
        "ResultCache",
        "query_instant",
        "GraphCore",
        "FlatIndex",
        "IvfPq",
        "HnswIndex",
        "HierarchicalRetriever",
        "plan_admissions",
        "SpanStore",
        "KnowledgeBatch",
        "ChangeNotifier",
        "exact_performance_probe_edge_ordinal",
    ):
        assert required_real_surface in compiled_probe_source
