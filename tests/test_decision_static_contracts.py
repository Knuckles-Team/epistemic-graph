"""No-engine acceptance for canonical solve DTOs and component closure docs."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine
ROOT = Path(__file__).resolve().parents[1]
CANONICAL = "crates/eg-types/src/solve/"
DECLARATION = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum|type)\s+(\w+)", re.M
)
# Reviewed homonyms: SQL ACL relations, pipeline model configuration,
# assembly model-profile fixtures, epistemic/RDF proofs, and mining selectors.
# Each exact file may define its homonym once; a second is still a violation.
HOMONYMS = {
    ("Relation", "src/server/sql_catalog_acl/relations.rs"),
    ("ModelSpec", "crates/eg-types/src/wire_pipeline.rs"),
    ("ModelSpec", "crates/eg-compute/src/assemble/tests/fixture.rs"),
    ("ProofNode", "crates/eg-epistemic/src/model.rs"),
    ("ProofNode", "crates/eg-rdf/src/owl.rs"),
    *(
        ("Algorithm", f"crates/eg-compute/src/mining/{name}.rs")
        for name in (
            "association",
            "forecast",
            "text",
            "anomaly",
            "classify",
            "reduce",
            "cluster",
            "sequence",
            "community",
        )
    ),
}


def _declarations(source: str) -> list[str]:
    return DECLARATION.findall(source)


def _canonical_names(sources: dict[str, str]) -> set[str]:
    return {
        name
        for path, source in sources.items()
        if path.startswith(CANONICAL)
        for name in re.findall(r"\bpub\s+(?:struct|enum|type)\s+(\w+)", source)
    }


def _declared_outside_canonical(
    sources: dict[str, str], names: set[str]
) -> Counter[tuple[str, str]]:
    return Counter(
        (name, path)
        for path, source in sources.items()
        if not path.startswith(CANONICAL)
        for name in _declarations(source)
        if name in names
    )


def _duplicate_dtos(sources: dict[str, str]) -> list[tuple[str, str]]:
    names = _canonical_names(sources)
    assert {"SolveRequest", "SolveResult", "Certificate", "ModelSpec"} <= names
    declarations = _declared_outside_canonical(sources, names)
    return [
        key
        for key, count in declarations.items()
        if count > (1 if key in HOMONYMS else 0)
    ]


def _sources() -> dict[str, str]:
    return {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for directory in ("src", "crates")
        for path in (ROOT / directory).rglob("*.rs")
    }


@pytest.mark.spec("EG-DECISION-ENGINE-R123")
def test_solve_dtos_have_no_second_definition() -> None:
    assert not _duplicate_dtos(_sources())


@pytest.mark.parametrize(
    "declaration",
    [
        "pub struct SolveRequest;",
        "enum Certificate {}",
        "type ModelSpec = ();",
        "pub(crate) struct Scalar(i128);",
    ],
)
def test_duplicate_scan_refuses_added_definitions(declaration: str) -> None:
    sources = {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in (ROOT / CANONICAL).glob("*.rs")
    }
    sources["crates/other/src/renamed.rs"] = declaration
    assert _duplicate_dtos(sources)


def _assert_kind_closure(component: str, slots: str) -> None:
    rule = component.split("/// What kind of part this is.", 1)[1].split("#[derive", 1)[
        0
    ]
    body = component.split("pub enum AgentComponentKind {", 1)[1].split("\n}", 1)[0]
    kinds = set(re.findall(r"^    (\w+),$", body, re.M))
    documented = set(re.findall(r"`([A-Z]\w+)`", rule))
    referenced = set(re.findall(r"AgentComponentKind::(\w+)", slots))
    assert kinds and referenced
    assert documented == kinds, f"closure rule differs: {documented ^ kinds}"
    assert referenced <= documented, (
        f"undocumented slot kinds: {referenced - documented}"
    )
    assert {
        "Shapes",
        "A2aAgentCard",
        "DecisionRecord",
        "DecisionPolicy",
        "DecisionHead",
    } <= documented
    assert "catalog content" in rule


def _kind_sources() -> tuple[str, str]:
    component = (ROOT / "crates/eg-types/src/agent_component.rs").read_text(
        encoding="utf-8"
    )
    paths = [
        ROOT / "crates/eg-types/src/agent_library.rs",
        ROOT / "crates/eg-types/src/agent_graph.rs",
    ]
    paths.extend((ROOT / "crates/eg-types/src/agent_graph").rglob("*.rs"))
    return component, "\n".join(path.read_text(encoding="utf-8") for path in paths)


@pytest.mark.spec("EG-DECISION-ENGINE-R074")
def test_agent_slot_kinds_are_documented_in_closure_rule() -> None:
    _assert_kind_closure(*_kind_sources())


@pytest.mark.parametrize("kind", ["Schema", "Shapes", "A2aAgentCard", "DecisionRecord"])
def test_closure_check_refuses_missing_documented_kind(kind: str) -> None:
    component, slots = _kind_sources()
    with pytest.raises(AssertionError, match="closure rule differs"):
        _assert_kind_closure(component.replace(f"`{kind}`", kind), slots)


def test_closure_check_refuses_new_slot_kind() -> None:
    component, slots = _kind_sources()
    with pytest.raises(AssertionError, match="undocumented slot kinds"):
        _assert_kind_closure(component, slots + "\nAgentComponentKind::NewSlotKind")


def test_duplicate_scan_refuses_second_declaration_in_homonym_file() -> None:
    sources = _sources()
    sources["crates/eg-types/src/wire_pipeline.rs"] += "\npub struct ModelSpec {}\n"
    assert ("ModelSpec", "crates/eg-types/src/wire_pipeline.rs") in _duplicate_dtos(
        sources
    )
