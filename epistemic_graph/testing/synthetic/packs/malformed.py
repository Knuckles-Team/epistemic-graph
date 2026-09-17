"""One planted defect per validation rule (PACK-IMPORT-DESIGN §3.10.1, §10).

Every malformed pack differs from a well-formed base by exactly the planted
defect. Unless the rule is about digests or sections, the producer's digests are
recomputed after the edit, so the import must report the planted rule rather
than a digest mismatch. ``expected_any`` lists the codes of which at least one
must be reported; ``permitted`` lists codes that may accompany them.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any, Literal

from pydantic import Field

from .._model import SyntheticModel
from .generate import pack_spec
from .model import BuiltPack
from .spec import EntrySpec, PackSpec, assemble


class MalformedPack(SyntheticModel):
    rule: str = Field(pattern=r"^G([1-9]|1[0-9]|2[01])$")
    variant: str
    description: str
    pack: BuiltPack
    expected_any: tuple[str, ...] = ()
    permitted: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    accepted: bool = False
    upload_archive: bool = True
    importer: Literal["configured", "unconfigured"] = "configured"
    prior: BuiltPack | None = None
    budget_dependent: bool = False
    max_reported_violations: int | None = None


Mutation = Callable[[PackSpec], MalformedPack]


def first(spec: PackSpec, kind: str) -> EntrySpec:
    return next(entry for entry in spec.entries if entry.kind == kind)


def swap(spec: PackSpec, old: EntrySpec, new: EntrySpec) -> PackSpec:
    return spec.with_entries([new if e is old else e for e in spec.entries])


def edited(spec: PackSpec, kind: str, **changes: Any) -> BuiltPack:
    """Assemble ``spec`` with the first entry of ``kind`` changed."""
    entry = first(spec, kind)
    return assemble(swap(spec, entry, replace(entry, **changes)))


def added(spec: PackSpec, *entries: EntrySpec) -> BuiltPack:
    return assemble(spec.with_entries([*spec.entries, *entries]))


def case(
    rule: str,
    variant: str,
    description: str,
    pack: BuiltPack,
    expected: Sequence[str] = (),
    **extra: object,
) -> MalformedPack:
    return MalformedPack.model_validate(
        {
            "rule": rule,
            "variant": variant,
            "description": description,
            "pack": pack,
            "expected_any": tuple(expected),
            **extra,
        }
    )


def base_spec(seed: int = 0) -> PackSpec:
    return pack_spec(seed, "typical")
