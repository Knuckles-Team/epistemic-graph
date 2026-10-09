"""Export the synthetic suite as labelled decision datasets (EH-055).

Two exports, one per label regime (DECIDE-LAYER-DESIGN §6.2):

* :func:`export_gold_dataset` turns an assembly :class:`GoldSet` into a
  FULL-LABEL dataset: one item per solvable plan, its options the plan's
  selectable candidates, its acceptability set every component of an
  acceptable assembly. The labels are ground truth by construction, so the
  source is ``synthetic_construction`` and the dataset says ``synthetic``.
* :func:`export_gold_corpus` merges the gold sets of several seeds into one
  full-label training corpus for the resident decision scorer (EH-302);
  ``python -m epistemic_graph.testing.synthetic.decision_export OUT SEED...``
  writes it as JSON for the offline ``decision_scorer_train`` example.
* :func:`export_outcome_dataset` turns an :class:`OutcomeStream` into a
  BANDIT-LABEL dataset: one item per logged record, carrying the exact logging
  row as propensities and every planted defect mapped onto the field the
  engine's admission rule (EH-016) checks, so a defect is refused by the
  engine rather than filtered out here.

Rows are ``Q32`` integers computed with exact integer arithmetic, in the same
column order as :data:`ASSEMBLY_FEATURE_SCHEMA` / :data:`OUTCOME_FEATURE_SCHEMA`,
whose content digests the datasets pin.

Training the resident decision scorer from this corpus (EG-DECISION-ENGINE-
R093) requires every item it trains on to carry a *verifiable* ground-truth
label, not merely a label. :data:`SOURCE_SERVED` marks an item whose
acceptability set was produced by driving the real served request path (the
engine that will run the trained scorer), distinct from :data:`SOURCE_IN_PROCESS`
construction; :func:`verify_ground_truth` refuses an item that claims the
served-path source but whose acceptable set is empty or not a subset of its
own candidates -- an unverifiable label a training run must not silently
learn from.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ... import decision_stat as ds
from . import vocabulary as vocab
from .assembly import Candidate, GoldSet, PlanItem, Solved
from .assembly_generate import generate_gold_set
from .outcomes import OutcomeRecord, OutcomeStream

Q32 = ds.Q32_ONE
MAX_DATASET_ITEMS = 4_096
#: A gold item's acceptability set was built in-process, from the generator's
#: own model of the plan (the pre-existing, historical source).
SOURCE_IN_PROCESS = "synthetic_construction"
#: A gold item's acceptability set was produced by driving the real served
#: request path (EG-DECISION-ENGINE-R093) -- the same engine the trained
#: scorer runs inside, rather than the generator's in-process model of it.
SOURCE_SERVED = "synthetic_served"


class UnverifiableGroundTruthError(ValueError):
    """An item claims :data:`SOURCE_SERVED` provenance but its label carries
    no verifiable ground truth. Training on it would silently teach the
    resident scorer from noise instead of from the served engine's own
    answer (EG-DECISION-ENGINE-R093)."""


def verify_ground_truth(item: Mapping[str, Any]) -> None:
    """Refuse ``item`` if its label claims :data:`SOURCE_SERVED` provenance
    without a verifiable acceptability set: one that is non-empty and wholly
    contained in the item's own ``candidate_ids``. An item sourced
    ``SOURCE_IN_PROCESS`` (or any other source) is not this function's
    concern and always passes -- only a served-path claim is held to this
    bar, because only it stands in for the engine's served ground truth."""
    label = item["label"]
    if label.get("source") != SOURCE_SERVED:
        return
    item_id = item["item_id"]
    acceptable = label.get("acceptable") or []
    if not acceptable:
        raise UnverifiableGroundTruthError(
            f"{item_id}: served-path item has no acceptable set to train on"
        )
    candidates = set(item["candidate_ids"])
    if not set(acceptable) <= candidates:
        raise UnverifiableGroundTruthError(
            f"{item_id}: acceptable set {sorted(set(acceptable) - candidates)} "
            "is not a subset of this item's own candidates"
        )


ASSEMBLY_FEATURE_SCHEMA = ds.feature_schema_body(
    [
        ds.feature("coverage", ds.coverage_fraction("needs")),
        ds.feature("cost", ds.unit_feature("declared_cost_micros"), impute=-1),
        ds.feature("p95", ds.unit_feature("declared_p95_latency_ms")),
    ]
)
OUTCOME_FEATURE_SCHEMA = ds.feature_schema_body(
    [ds.feature("option_index", ds.number("option_index"))]
)
_FIDELITY = {
    "full-step": "full_step",
    "tool-calls": "tool_calls",
    "final-output": "final_output",
    "trace-incomplete": "trace_incomplete",
    "outcome-uncertain": "outcome_uncertain",
    "cancelled": "cancelled",
}
_CENSORED = ("trace-incomplete", "outcome-uncertain", "cancelled")
COMMIT_PRINCIPAL = "synthetic-principal"


def schema_digest(schema: dict[str, Any]) -> str:
    return ds.encode_body(schema)[0]


def _dataset(
    schema: dict[str, Any], names: Sequence[str], items: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "schema_version": ds.LABELLED_DATASET_SCHEMA_VERSION,
        "feature_schema_digest": schema_digest(schema),
        "feature_names": list(names),
        "scale": "q32",
        "items": items,
        "synthetic": True,
    }


def _coverage(classification: Sequence[str], required: Sequence[str]) -> int:
    covered = sum(
        1
        for need in required
        if any(vocab.satisfies(have, need) for have in classification)
    )
    return covered * Q32 // len(required)


def _published_options(item: PlanItem) -> list[Candidate]:
    """The item's published candidates, in component-id byte order."""
    return sorted(
        (c for c in item.candidates if c.lifecycle == "published"),
        key=lambda c: c.component_id.encode(),
    )


def _feature_row(candidate: Candidate, required: Sequence[str]) -> list[int]:
    """One candidate's (coverage, cost, p95) features in Q32; an unknown cost
    is -1."""
    cost = -1 if candidate.cost_micros is None else candidate.cost_micros
    return [
        _coverage(candidate.classification, required),
        cost * Q32,
        candidate.p95_ms * Q32,
    ]


def _gold_item(item: PlanItem, solved: Solved) -> dict[str, Any] | None:
    options = _published_options(item)
    accepted = {member for assembly in solved.acceptable for member in assembly}
    acceptable = [c.component_id for c in options if c.component_id in accepted]
    if not acceptable:
        return None
    rows = [
        value for c in options for value in _feature_row(c, item.required_capabilities)
    ]
    return {
        "item_id": item.item_id,
        "recorded_at_ms": 0,
        "class_key": item.task or "capabilities",
        "candidate_ids": [c.component_id for c in options],
        "features": rows,
        "label": {
            "label": "gold",
            "acceptable": acceptable,
            "source": SOURCE_IN_PROCESS,
        },
        "audit_inclusion": None,
    }


def export_gold_dataset(gold: GoldSet) -> dict[str, Any]:
    """The full-label dataset of every solvable plan in ``gold``."""
    items = [
        exported
        for item in gold.items
        if isinstance(item.expected, Solved)
        and (exported := _gold_item(item, item.expected))
    ]
    return _dataset(ASSEMBLY_FEATURE_SCHEMA, ("coverage", "cost", "p95"), items)


def export_gold_corpus(seeds: Sequence[int]) -> dict[str, Any]:
    """One full-label corpus over the gold sets of ``seeds``, in seed order.

    Item ids are prefixed with their seed so they stay unique, and each item
    is recorded at its seed's index, so a promotion evaluation can measure
    calibration drift across seeds. Truncated at the dataset bound.
    """
    items: list[dict[str, Any]] = []
    for index, seed in enumerate(seeds):
        for item in export_gold_dataset(generate_gold_set(seed))["items"]:
            items.append(
                {
                    **item,
                    "item_id": f"seed-{seed}-{item['item_id']}",
                    "recorded_at_ms": index,
                }
            )
    names = ("coverage", "cost", "p95")
    return _dataset(ASSEMBLY_FEATURE_SCHEMA, names, items[:MAX_DATASET_ITEMS])


def _evaluation(record: OutcomeRecord) -> dict[str, Any]:
    independent = record.independent_evaluator
    return {
        "evaluation_id": f"evaluation-{record.record_id}",
        "class": record.evaluation_class,
        "producer": "independent-evaluator" if independent else "selected-agent",
        "selected_agent": "selected-agent",
        "lease_holder": "lease-holder",
        "fidelity": _FIDELITY[record.fidelity],
        "success": None if record.fidelity in _CENSORED else record.success,
    }


def _logged_item(stream: OutcomeStream, record: OutcomeRecord) -> dict[str, Any]:
    row = sorted(
        (c for c in stream.logging.cells if c.context_id == record.context_id),
        key=lambda c: c.option_id.encode(),
    )
    options = [c.option_id for c in row]
    propensities = [
        {"numerator": c.probability.numerator, "denominator": c.probability.denominator}
        for c in row
    ]
    return {
        "item_id": record.record_id,
        "recorded_at_ms": 0,
        "class_key": record.context_id,
        "candidate_ids": options,
        "features": [index * Q32 for index in range(len(options))],
        "label": {
            "label": "logged",
            "executed": record.option_id,
            "logging_propensities": propensities,
            "propensity_source": "head_mass"
            if record.defect == "propensity_is_head_mass"
            else "executed_policy",
            "pinned": record.defect == "pinned",
            "commit_principal": COMMIT_PRINCIPAL,
            "evaluation": _evaluation(record),
        },
        "audit_inclusion": None,
    }


def export_outcome_dataset(stream: OutcomeStream) -> dict[str, Any]:
    """The bandit-label dataset of every logged record, defects included."""
    items = [_logged_item(stream, record) for record in stream.records]
    return _dataset(OUTCOME_FEATURE_SCHEMA, ("option_index",), items)


def main(argv: Sequence[str]) -> int:
    """``OUT SEED...``: write the gold corpus of the seeds to ``OUT``."""
    if len(argv) < 2:
        sys.stderr.write("usage: decision_export OUT SEED [SEED ...]\n")
        return 2
    corpus = export_gold_corpus([int(seed) for seed in argv[1:]])
    Path(argv[0]).write_text(json.dumps(corpus, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
