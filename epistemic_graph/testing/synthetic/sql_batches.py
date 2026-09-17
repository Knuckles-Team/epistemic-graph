"""(e) SQL source batch sequences with planted accept/refuse outcomes.

One source partition feeds one table. Batches carry cursor positions with a
compare-and-swap on the previous position (the SQL source branch's checkpoint
rules). Interleaved with the accepted sequence are an idempotent replay, a
reused operation key with different content, stale and missing
``expected_previous``, a changed mapping, a stale schema digest, a duplicate
primary key and client-side shape errors. None of the refused steps may add rows
or move the cursor; the planted final table and cursor say so.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from ._digest import canonical_json
from ._model import Provenance, SyntheticModel
from ._rng import SeededStream

GENERATOR = "sql-batches"
GENERATOR_VERSION = 1
UNKNOWN_SCHEMA_DIGEST = "00" * 32

Expectation = Literal[
    "committed",
    "replayed",
    "idempotency_conflict",
    "cas_conflict",
    "binding_conflict",
    "schema_conflict",
    "duplicate_key",
    "client_invalid",
]


class Column(SyntheticModel):
    name: str
    sql_type: Literal["BIGINT", "TEXT", "JSON", "BOOLEAN"]
    nullable: bool
    primary_key: bool = False


class Table(SyntheticModel):
    name: str
    columns: tuple[Column, ...]

    def ddl(self) -> str:
        parts = [
            " ".join(
                [c.name, c.sql_type]
                + (["PRIMARY KEY"] if c.primary_key else [])
                + ([] if c.nullable else ["NOT NULL"])
            )
            for c in self.columns
        ]
        return f"CREATE TABLE {self.name} ({', '.join(parts)})"


class Row(SyntheticModel):
    issue_id: int
    owner_tag: str
    payload: str | None
    open: bool


class Step(SyntheticModel):
    index: int = Field(ge=0)
    label: str
    idempotency_key: str
    expect: Expectation
    rows: tuple[Row, ...]
    batch: dict[str, Any]


class SqlScenario(SyntheticModel):
    provenance: Provenance
    table: Table
    source: str
    partition: str
    steps: tuple[Step, ...]
    final_rows: tuple[Row, ...]
    final_position: int

    def committed(self) -> tuple[Step, ...]:
        return tuple(step for step in self.steps if step.expect == "committed")


TABLE = Table(
    name="synthetic_issues",
    columns=(
        Column(name="issue_id", sql_type="BIGINT", nullable=False, primary_key=True),
        Column(name="owner_tag", sql_type="TEXT", nullable=False),
        Column(name="payload", sql_type="JSON", nullable=True),
        Column(name="open", sql_type="BOOLEAN", nullable=False),
    ),
)


def _cells(row: Row) -> list[dict[str, Any]]:
    payload: dict[str, Any] = (
        {"kind": "null"}
        if row.payload is None
        else {"kind": "json", "value": row.payload.encode()}
    )
    return [
        {"kind": "int", "value": row.issue_id},
        {"kind": "text", "value": row.owner_tag},
        payload,
        {"kind": "bool", "value": row.open},
    ]


def wire_batch(
    rows: tuple[Row, ...],
    position: int,
    previous: int | None,
    *,
    mapping: bytes = b"issue.id -> issue_id",
    schema_digest: str = UNKNOWN_SCHEMA_DIGEST,
    schema_version: int = 0,
    partition: str = "project-a",
) -> dict[str, Any]:
    """The `SqlSourceBatch` wire value, as the SQL branch's client accepts it."""
    return {
        "source": "synthetic-tracker",
        "partition": partition,
        "position": {"kind": "sequence", "value": position},
        "expected_previous": None
        if previous is None
        else {"kind": "sequence", "value": previous},
        "source_descriptor": {
            "provider": "synthetic-tracker",
            "dataset": "issues",
            "metadata": canonical_json({"deployment": "synthetic"}),
        },
        "mapping_descriptor": {"format": "text", "content": mapping},
        "table": TABLE.name,
        "columns": [column.name for column in TABLE.columns],
        "rows": [_cells(row) for row in rows],
        "expected_schema_version": schema_version,
        "expected_schema_digest": schema_digest,
    }


def synthetic_rows(stream: SeededStream, first_id: int, count: int) -> tuple[Row, ...]:
    return tuple(
        Row(
            issue_id=first_id + offset,
            owner_tag=stream.choice(("alpha", "beta", "gamma")),
            payload=None
            if stream.chance(1, 4)
            else canonical_json({"n": first_id + offset}).decode(),
            open=stream.chance(1, 2),
        )
        for offset in range(count)
    )
