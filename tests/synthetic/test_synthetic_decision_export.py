"""EH-055: the synthetic suite exports as labelled decision datasets."""

from __future__ import annotations

import pytest

from epistemic_graph import decision_stat as ds
from epistemic_graph.testing.synthetic import decision_export as export
from epistemic_graph.testing.synthetic.assembly import Solved
from epistemic_graph.testing.synthetic.assembly_generate import generate_gold_set
from epistemic_graph.testing.synthetic.outcomes_generate import generate_outcome_stream

pytestmark = pytest.mark.no_engine


def test_the_gold_set_exports_one_full_label_item_per_solvable_plan() -> None:
    gold = generate_gold_set(0)
    dataset = export.export_gold_dataset(gold)
    solvable = [item for item in gold.items if isinstance(item.expected, Solved)]
    assert len(dataset["items"]) == len(solvable)
    assert dataset["synthetic"] is True
    assert dataset["feature_schema_digest"] == export.schema_digest(
        export.ASSEMBLY_FEATURE_SCHEMA
    )
    for item in dataset["items"]:
        width = len(dataset["feature_names"])
        assert len(item["features"]) == width * len(item["candidate_ids"])
        assert set(item["label"]["acceptable"]) <= set(item["candidate_ids"])
        assert item["label"]["source"] == "synthetic_construction"
    assert ds.dataset_digest(dataset) == ds.dataset_digest(
        export.export_gold_dataset(gold)
    )


def _marked(label: dict) -> bool:
    evaluation = label["evaluation"]
    return (
        label["pinned"]
        or label["propensity_source"] != "executed_policy"
        or evaluation["class"] != "observation"
        or evaluation["producer"] == evaluation["selected_agent"]
        or evaluation["success"] is None
        or evaluation["fidelity"] not in ("full_step", "tool_calls")
    )


def test_every_planted_defect_reaches_the_field_the_engine_refuses_on() -> None:
    stream = generate_outcome_stream(0, records=300, defects_each=5)
    dataset = export.export_outcome_dataset(stream)
    assert len(dataset["items"]) == len(stream.records)
    unmarked = [item for item in dataset["items"] if not _marked(item["label"])]
    assert len(unmarked) == len(stream.admissible_records())
    for item in dataset["items"]:
        propensities = item["label"]["logging_propensities"]
        assert len(propensities) == len(item["candidate_ids"])
        assert item["label"]["executed"] in item["candidate_ids"]
