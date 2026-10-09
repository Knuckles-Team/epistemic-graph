"""EH-055 / EH-302: the synthetic suite exports as labelled decision datasets."""

from __future__ import annotations

import json
from pathlib import Path

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
        assert item["label"]["source"] == export.SOURCE_IN_PROCESS
        export.verify_ground_truth(item)
    assert ds.dataset_digest(dataset) == ds.dataset_digest(
        export.export_gold_dataset(gold)
    )


def _served_item(acceptable: list[str], candidate_ids: list[str]) -> dict:
    return {
        "item_id": "served-0",
        "candidate_ids": candidate_ids,
        "label": {"source": export.SOURCE_SERVED, "acceptable": acceptable},
    }


def test_verify_ground_truth_accepts_a_served_item_with_a_grounded_acceptable_set() -> (
    None
):
    item = _served_item(["a"], ["a", "b"])
    export.verify_ground_truth(item)  # does not raise


def test_verify_ground_truth_ignores_a_non_served_source() -> None:
    item = _served_item([], ["a", "b"])
    item["label"]["source"] = export.SOURCE_IN_PROCESS
    export.verify_ground_truth(item)  # does not raise: not a served-path claim


def test_verify_ground_truth_refuses_a_served_item_with_no_acceptable_set() -> None:
    item = _served_item([], ["a", "b"])
    with pytest.raises(export.UnverifiableGroundTruthError, match="no acceptable set"):
        export.verify_ground_truth(item)


def test_verify_ground_truth_refuses_a_served_item_whose_acceptable_set_leaks() -> None:
    item = _served_item(["a", "ghost"], ["a", "b"])
    with pytest.raises(export.UnverifiableGroundTruthError, match="not a subset"):
        export.verify_ground_truth(item)


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


def test_the_gold_corpus_merges_seeds_with_unique_ids_and_seed_times(
    tmp_path: Path,
) -> None:
    corpus = export.export_gold_corpus([0, 1])
    ids = [item["item_id"] for item in corpus["items"]]
    assert len(ids) == len(set(ids))
    first = len(export.export_gold_dataset(generate_gold_set(0))["items"])
    assert {item["recorded_at_ms"] for item in corpus["items"][:first]} == {0}
    assert {item["recorded_at_ms"] for item in corpus["items"][first:]} == {1}
    out = tmp_path / "corpus.json"
    assert export.main([str(out), "0", "1"]) == 0
    assert ds.dataset_digest(json.loads(out.read_text())) == ds.dataset_digest(corpus)
