"""Digest framing, the deterministic stream and the vocabulary mirror."""

from __future__ import annotations

import hashlib
import re
import struct
from pathlib import Path

import pytest

from epistemic_graph.testing.synthetic import vocabulary as vocab
from epistemic_graph.testing.synthetic._digest import canonical_json, framed
from epistemic_graph.testing.synthetic._rng import SeededStream

pytestmark = pytest.mark.no_engine

ONTOLOGY_RS = (
    Path(__file__).resolve().parents[2] / "crates/eg-types/src/agent_ontology.rs"
)
ROOT_CONSTANTS = {
    "CAPABILITY_ROOT": vocab.CAPABILITY_ROOT,
    "MODALITY_ROOT": vocab.MODALITY_ROOT,
    "TASK_ROOT": vocab.TASK_ROOT,
}


def test_framed_matches_a_hand_built_frame() -> None:
    frame = b"eg/framed-sha256/v1\x00" + struct.pack(">I", 7) + b"test/v1"
    frame += struct.pack(">I", 3)
    for field in (b"ab", b"", b"c"):
        frame += struct.pack(">Q", len(field)) + field
    assert framed(b"test/v1", [b"ab", b"", b"c"]) == hashlib.sha256(frame).digest()
    assert framed(b"test/v1", [b"ab", b"c"]) != framed(b"test/v1", [b"a", b"bc"])


def test_framed_refuses_an_empty_domain() -> None:
    with pytest.raises(ValueError):
        framed(b"", [b"x"])


def test_stream_is_pinned_and_labels_are_independent() -> None:
    # Golden values: a change here changes every generated fixture.
    stream = SeededStream(42, "golden")
    assert [stream.next_u64() for _ in range(2)] == [
        int.from_bytes(
            framed(
                b"eg/synthetic-stream/v1",
                [struct.pack(">Q", 42), b"golden", struct.pack(">Q", i)],
            )[:8],
            "big",
        )
        for i in range(2)
    ]
    parent = SeededStream(42, "golden")
    parent.next_u64()
    assert parent.child("x").next_u64() == SeededStream(42, "golden/x").next_u64()


@pytest.mark.parametrize("bound", [1, 2, 3, 10, 1 << 40])
def test_below_stays_in_range(bound: int) -> None:
    stream = SeededStream(1, f"below-{bound}")
    assert all(0 <= stream.below(bound) < bound for _ in range(200))


def test_chance_extremes_and_permutations() -> None:
    stream = SeededStream(2, "chance")
    assert not any(stream.chance(0, 5) for _ in range(50))
    assert all(stream.chance(5, 5) for _ in range(50))
    items = list(range(30))
    shuffled = stream.shuffled(items)
    assert sorted(shuffled) == items and shuffled != items
    assert len(set(stream.sample(items, 10))) == 10
    with pytest.raises(ValueError):
        stream.sample(items, 31)


def test_canonical_json_is_the_sdk_rule() -> None:
    assert canonical_json({"b": 1, "a": "é"}) == '{"a":"é","b":1}'.encode()
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def _rust_terms() -> dict[str, tuple[str | None, tuple[str, ...]]]:
    """(broader, requires) per IRI, read from the compiled Rust table."""
    text = " ".join(ONTOLOGY_RS.read_text().split())
    table = text[text.index("pub const AGENT_ONTOLOGY") : text.index("const fn t(")]

    def resolve(token: str) -> str:
        token = token.strip().strip('"')
        return ROOT_CONSTANTS.get(token, token)

    terms: dict[str, tuple[str | None, tuple[str, ...]]] = {}
    for match in re.finditer(
        r'\bt\(\s*("[^"]+"|\w+),\s*"[^"]*",\s*(None|Some\(\s*("[^"]+"|\w+)\s*\))\s*,?\s*\)',
        table,
    ):
        broader = None if match.group(2) == "None" else resolve(match.group(3))
        terms[resolve(match.group(1))] = (broader, ())
    for match in re.finditer(
        r'\btask\(\s*"([^"]+)",\s*"[^"]*",\s*&\[([^\]]*)\]', table
    ):
        needs = tuple(resolve(n) for n in match.group(2).split(",") if n.strip())
        terms[match.group(1)] = (vocab.TASK_ROOT, needs)
    return terms


def test_vocabulary_mirrors_the_rust_table() -> None:
    mirror = {term.iri: (term.broader, term.requires) for term in vocab.TERMS}
    assert mirror == _rust_terms()


def test_subsumption_is_directional() -> None:
    assert vocab.satisfies(
        "eg:capability/retrieval/web-search", "eg:capability/retrieval"
    )
    assert not vocab.satisfies(
        "eg:capability/retrieval", "eg:capability/retrieval/web-search"
    )
    assert not vocab.satisfies(
        "https://synthetic.example/capability/x", "eg:capability"
    )
    assert vocab.is_side_effecting_term("eg:capability/action/schedule")
    assert len(vocab.leaves(vocab.CAPABILITY_ROOT)) == 25
