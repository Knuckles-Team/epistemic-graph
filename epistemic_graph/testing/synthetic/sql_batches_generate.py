"""Seeded construction of SQL source scenarios (see `sql_batches`)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ._model import Provenance
from ._rng import SeededStream
from .sql_batches import (
    GENERATOR,
    GENERATOR_VERSION,
    TABLE,
    UNKNOWN_SCHEMA_DIGEST,
    Expectation,
    Row,
    SqlScenario,
    Step,
    synthetic_rows,
    wire_batch,
)


@dataclass
class _Plan:
    stream: SeededStream
    schema_digest: str
    schema_version: int
    steps: list[Step] = field(default_factory=list)
    head: int = 0
    next_id: int = 1

    def add(
        self,
        label: str,
        expect: Expectation,
        rows: tuple[Row, ...],
        batch: dict[str, Any],
        key: str | None = None,
    ) -> Step:
        step = Step(
            index=len(self.steps),
            label=label,
            idempotency_key=key or f"synthetic-sql-{len(self.steps):03d}",
            expect=expect,
            rows=rows,
            batch=batch,
        )
        self.steps.append(step)
        return step

    def batch(
        self,
        rows: tuple[Row, ...],
        position: int,
        previous: int | None,
        **overrides: Any,
    ) -> dict[str, Any]:
        options: dict[str, Any] = {
            "schema_digest": self.schema_digest,
            "schema_version": self.schema_version,
        }
        return wire_batch(rows, position, previous, **(options | overrides))

    def accept(self, label: str) -> Step:
        rows = synthetic_rows(
            self.stream.child(label), self.next_id, self.stream.between(1, 8)
        )
        previous = self.head or None
        step = self.add(
            label, "committed", rows, self.batch(rows, self.head + 1, previous)
        )
        self.head += 1
        self.next_id += len(rows)
        return step

    def refuse(
        self,
        label: str,
        expect: Expectation,
        position: int,
        previous: int | None,
        **overrides: Any,
    ) -> None:
        rows = synthetic_rows(self.stream.child(label), self.next_id + 1_000, 1)
        self.add(label, expect, rows, self.batch(rows, position, previous, **overrides))


def _conflicts(plan: _Plan, replayed: Step, keyed: Step) -> None:
    head = plan.head
    plan.add(
        "replay_same_operation",
        "replayed",
        replayed.rows,
        replayed.batch,
        replayed.idempotency_key,
    )
    altered = dict(
        keyed.batch,
        mapping_descriptor={"format": "text", "content": b"altered mapping"},
    )
    plan.add(
        "reused_key_new_content",
        "idempotency_conflict",
        keyed.rows,
        altered,
        keyed.idempotency_key,
    )
    plan.refuse("stale_expected_previous", "cas_conflict", head + 1, head - 1)
    plan.refuse(
        "first_batch_claims_previous", "cas_conflict", 2, 1, partition="project-b"
    )
    plan.refuse(
        "mapping_changed",
        "binding_conflict",
        head + 1,
        head,
        mapping=b"issue.key -> issue_id",
    )
    # With no checkpoint yet, the stale digest meets the table schema CAS itself
    # rather than the checkpoint's recorded binding.
    plan.refuse(
        "stale_schema_digest",
        "schema_conflict",
        1,
        None,
        schema_digest="ff" * 32,
        partition="project-c",
    )
    duplicate = replayed.rows[:1]
    plan.add(
        "duplicate_primary_key",
        "duplicate_key",
        duplicate,
        plan.batch(duplicate, head + 1, head),
    )
    plan.refuse("position_not_advancing", "client_invalid", head, head)
    bad_width = plan.batch(replayed.rows[:1], head + 1, head)
    bad_width["rows"] = [row[:-1] for row in bad_width["rows"]]
    plan.add("row_width_mismatch", "client_invalid", replayed.rows[:1], bad_width)
    doubled = plan.batch(replayed.rows[:1], head + 1, head)
    doubled["columns"] = [*doubled["columns"][:-1], doubled["columns"][0]]
    plan.add("duplicate_column_names", "client_invalid", replayed.rows[:1], doubled)


def generate_sql_scenario(
    seed: int, *, schema_digest: str = UNKNOWN_SCHEMA_DIGEST, schema_version: int = 0
) -> SqlScenario:
    """Five accepted batches, every refusal kind, then one more accepted batch.

    ``schema_digest``/``schema_version`` are the target table's, read from the
    engine catalog by the end-to-end scenario before the batches are built.
    """
    plan = _Plan(SeededStream(seed, GENERATOR), schema_digest, schema_version)
    accepted = [plan.accept(f"accept_{n}") for n in range(5)]
    _conflicts(plan, accepted[2], accepted[1])
    plan.accept("accept_after_refusals")
    committed = [step for step in plan.steps if step.expect == "committed"]
    return SqlScenario(
        provenance=Provenance(
            generator=GENERATOR, generator_version=GENERATOR_VERSION, seed=seed
        ),
        table=TABLE,
        source="synthetic-tracker",
        partition="project-a",
        steps=tuple(plan.steps),
        final_rows=tuple(row for step in committed for row in step.rows),
        final_position=plan.head,
    )
