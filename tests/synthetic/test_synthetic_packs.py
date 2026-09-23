"""The planted-pack generator is complete and speaks the engine's exact wire.

PA6's per-rule acceptance test (``tests/test_connector_pack_planted.py``)
imports every generated pack through a live engine. These engine-free checks
pin the two properties that test relies on: every validation rule G1-G21 has at
least one planted defect, and a well-formed generated pack carries exactly the
digest the published client (and therefore the engine) computes.
"""

from __future__ import annotations

from typing import get_args

import pytest

from epistemic_graph.connector_pack import pack_digest as client_pack_digest
from epistemic_graph.generated.connector_pack import (
    McpCatalogSnapshotBinding,
    PackEntry,
)
from epistemic_graph.testing.synthetic.packs.catalogue import all_malformed
from epistemic_graph.testing.synthetic.packs.generate import Profile, generate_pack

pytestmark = pytest.mark.no_engine


def test_every_rule_has_a_planted_defect() -> None:
    rules = {case.rule for case in all_malformed(0)}
    assert rules == {f"G{number}" for number in range(1, 22)}


def test_planted_variants_are_unique_per_rule() -> None:
    keys = [(case.rule, case.variant) for case in all_malformed(0)]
    assert len(keys) == len(set(keys))


@pytest.mark.parametrize("profile", get_args(Profile))
def test_generated_pack_digest_is_the_client_digest(profile: Profile) -> None:
    pack = generate_pack(0, profile)
    index = pack.index.model_dump(mode="json")
    expected = client_pack_digest(
        index["connector"],
        McpCatalogSnapshotBinding.model_validate(index["catalog"]),
        PackEntry.model_validate(index["server"]),
        [PackEntry.model_validate(entry) for entry in index["entries"]],
    )
    assert pack.index.pack_digest == expected


def test_the_typical_pack_carries_a_skill_supporting_file() -> None:
    kinds = {entry.kind for entry in generate_pack(0).index.entries}
    assert "skill_file" in kinds
