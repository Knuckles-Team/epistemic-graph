"""Pure OpenLineage identity and quarantine contract."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.openlineage_derivation import (
    MalformedLineageDataset,
    MappedRunEvent,
    QuarantinedLineageEvent,
    dataset_entity_id,
    map_openlineage_event,
    openlineage_activity_id,
    openlineage_activity_properties,
)

pytestmark = pytest.mark.no_engine


def _dataset(
    *,
    namespace: str = "iceberg://lakehouse/sales",
    name: str = "orders",
    snapshot: str = "snap-42",
) -> dict[str, Any]:
    facets: dict[str, Any] = {}
    if snapshot:
        facets["version"] = {"datasetVersion": snapshot}
    return {"namespace": namespace, "name": name, "facets": facets}


def _run_event(
    *,
    run_id: str = "01977c9e-0000-7000-8000-000000000001",
    job_name: str = "etl-orders",
    job_namespace: str = "spark",
    event_type: str = "COMPLETE",
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
    parent_run_id: str = "",
) -> dict[str, Any]:
    run: dict[str, Any] = {"runId": run_id}
    if parent_run_id:
        run["facets"] = {"parent": {"run": {"runId": parent_run_id}}}
    return {
        "eventType": event_type,
        "run": run,
        "job": {"namespace": job_namespace, "name": job_name},
        "inputs": inputs if inputs is not None else [],
        "outputs": outputs if outputs is not None else [],
    }


# ── dataset_entity_id ───────────────────────────────────────────────────────


def test_dataset_entity_id_builds_iceberg_uri() -> None:
    assert dataset_entity_id(_dataset()) == "iceberg://lakehouse/sales/orders@snap-42"


def test_dataset_entity_id_missing_version_facet_raises() -> None:
    with pytest.raises(MalformedLineageDataset):
        dataset_entity_id(_dataset(snapshot=""))


def test_dataset_entity_id_non_iceberg_namespace_raises() -> None:
    with pytest.raises(MalformedLineageDataset):
        dataset_entity_id(_dataset(namespace="postgres://db/public"))


def test_dataset_entity_id_missing_name_raises() -> None:
    with pytest.raises(MalformedLineageDataset):
        dataset_entity_id(_dataset(name=""))


# ── map_openlineage_event: happy path + RunTrace-correlation extraction ────


def test_map_openlineage_event_valid_creates_mapped_event() -> None:
    event = _run_event(
        inputs=[_dataset(name="raw_orders", snapshot="snap-7")],
        outputs=[_dataset(name="orders", snapshot="snap-42")],
        parent_run_id="01977c9e-0000-7000-8000-00000000ffff",
    )
    mapped = map_openlineage_event(event)
    assert isinstance(mapped, MappedRunEvent)
    assert mapped.run_id == "01977c9e-0000-7000-8000-000000000001"
    assert mapped.job_name == "etl-orders"
    assert mapped.job_namespace == "spark"
    assert mapped.event_type == "COMPLETE"
    assert mapped.activity_status == "completed"
    assert mapped.input_dataset_ids == ("iceberg://lakehouse/sales/raw_orders@snap-7",)
    assert mapped.output_dataset_ids == ("iceberg://lakehouse/sales/orders@snap-42",)
    assert mapped.parent_run_id == "01977c9e-0000-7000-8000-00000000ffff"


def test_map_openlineage_event_no_datasets_is_valid() -> None:
    mapped = map_openlineage_event(_run_event(event_type="START"))
    assert isinstance(mapped, MappedRunEvent)
    assert mapped.activity_status == "running"
    assert mapped.input_dataset_ids == ()
    assert mapped.output_dataset_ids == ()


@pytest.mark.parametrize(
    "event_type,expected_status",
    [
        ("START", "running"),
        ("RUNNING", "running"),
        ("COMPLETE", "completed"),
        ("ABORT", "aborted"),
        ("FAIL", "failed"),
        ("OTHER", "unknown"),
    ],
)
def test_map_openlineage_event_status_table(
    event_type: str, expected_status: str
) -> None:
    mapped = map_openlineage_event(_run_event(event_type=event_type))
    assert isinstance(mapped, MappedRunEvent)
    assert mapped.activity_status == expected_status


# ── map_openlineage_event: quarantine paths (acceptance gate 2) ───────────


def test_map_openlineage_event_missing_run_id_quarantined() -> None:
    event = _run_event()
    event["run"] = {}
    mapped = map_openlineage_event(event)
    assert isinstance(mapped, QuarantinedLineageEvent)
    assert "run.runId" in mapped.reason


def test_map_openlineage_event_missing_job_name_quarantined() -> None:
    event = _run_event()
    event["job"] = {"namespace": "spark"}
    mapped = map_openlineage_event(event)
    assert isinstance(mapped, QuarantinedLineageEvent)
    assert "job.name" in mapped.reason


def test_map_openlineage_event_unmapped_event_type_quarantined() -> None:
    mapped = map_openlineage_event(_run_event(event_type="RESURRECT"))
    assert isinstance(mapped, QuarantinedLineageEvent)
    assert "eventType" in mapped.reason


def test_map_openlineage_event_malformed_dataset_quarantines_whole_event() -> None:
    """A single unmapped-namespace dataset quarantines the WHOLE event — no
    partial mapping of the datasets that DID resolve (acceptance gate 2)."""
    event = _run_event(
        inputs=[_dataset(namespace="postgres://db/public", name="raw", snapshot="")],
        outputs=[_dataset(name="orders", snapshot="snap-42")],
    )
    mapped = map_openlineage_event(event)
    assert isinstance(mapped, QuarantinedLineageEvent)


def test_map_openlineage_event_correctly_namespaced_event_maps_normally() -> None:
    """The paired positive case for gate 2: a correctly-namespaced event maps
    to a normal MappedRunEvent, not a quarantine."""
    event = _run_event(outputs=[_dataset(name="orders", snapshot="snap-42")])
    mapped = map_openlineage_event(event)
    assert isinstance(mapped, MappedRunEvent)
    assert mapped.output_dataset_ids == ("iceberg://lakehouse/sales/orders@snap-42",)


def test_activity_id_stable_across_lifecycle_events() -> None:
    start = map_openlineage_event(_run_event(event_type="START"))
    complete = map_openlineage_event(_run_event(event_type="COMPLETE"))
    assert isinstance(start, MappedRunEvent)
    assert isinstance(complete, MappedRunEvent)
    assert openlineage_activity_id(start.run_id) == openlineage_activity_id(
        complete.run_id
    )
    assert openlineage_activity_id(start.run_id).startswith("activity:openlineage_run:")
    assert len(openlineage_activity_id(start.run_id).rsplit(":", 1)[-1]) == 64


def test_activity_properties_project_validated_status_and_explicit_time() -> None:
    mapped = map_openlineage_event(_run_event(event_type="COMPLETE"))
    assert isinstance(mapped, MappedRunEvent)
    assert openlineage_activity_properties(mapped, at=42.5) == {
        "kind": "openlineage_run",
        "job": "etl-orders",
        "jobNamespace": "spark",
        "eventType": "COMPLETE",
        "status": "completed",
        "at": 42.5,
    }
