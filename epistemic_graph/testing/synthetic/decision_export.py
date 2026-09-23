"""Export the synthetic suite as labelled decision datasets (EH-055).

Two exports, one per label regime (DECIDE-LAYER-DESIGN §6.2):

* :func:`export_gold_dataset` turns an assembly :class:`GoldSet` into a
  FULL-LABEL dataset: one item per solvable plan, its options the plan's
  selectable candidates, its acceptability set every component of an
  acceptable assembly. The labels are ground truth by construction, so the
  source is ``synthetic_construction`` and the dataset says ``synthetic``.
* :func:`export_outcome_dataset` turns an :class:`OutcomeStream` into a
  BANDIT-LABEL dataset: one item per logged record, carrying the exact logging
  row as propensities and every planted defect mapped onto the field the
  engine's admission rule (EH-016) checks, so a defect is refused by the
  engine rather than filtered out here.

Rows are ``Q32`` integers computed with exact integer arithmetic, in the same
column order as :data:`ASSEMBLY_FEATURE_SCHEMA` / :data:`OUTCOME_FEATURE_SCHEMA`,
whose content digests the datasets pin.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ... import decision_stat as ds
from . import vocabulary as vocab
from .assembly import GoldSet, PlanItem, Solved
from .outcomes import OutcomeRecord, OutcomeStream

Q32 = ds.Q32_ONE
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


def _gold_item(item: PlanItem, solved: Solved) -> dict[str, Any] | None:
    options = sorted(
        (c for c in item.candidates if c.lifecycle == "published"),
        key=lambda c: c.component_id.encode(),
    )
    accepted = {member for assembly in solved.acceptable for member in assembly}
    acceptable = [c.component_id for c in options if c.component_id in accepted]
    if not acceptable:
        return None
    rows: list[int] = []
    for c in options:
        cost = -1 if c.cost_micros is None else c.cost_micros
        rows += [
            _coverage(c.classification, item.required_capabilities),
            cost * Q32,
            c.p95_ms * Q32,
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
            "source": "synthetic_construction",
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
