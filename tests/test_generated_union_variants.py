"""Shared tagged unions keep their variant classes importable (EH-377 follow-up).

EH-377 renders a union reached by several surfaces once, in ``_shared``. A surface
module that re-exports the union must re-export its variant classes too, and so must
``models``: graph-os constructs ``ComponentProvenanceMcpServer`` from
``agent_component`` and AU checks the decision variants from ``decision``.
"""

from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.no_engine

CONSUMED = [
    ("agent_component", "ComponentProvenance", "ComponentProvenanceMcpServer"),
    ("decision", "AbstainReason", "AbstainReasonInsufficientConfidence"),
    ("decision", "CandidateSourceRecord", "CandidateSourceRecordAgentLibrary"),
    ("decision", "DecisionOutcome", "DecisionOutcomeAbstained"),
]


@pytest.mark.spec("MEDULLA-CONTRACT-R005")
@pytest.mark.parametrize(("module", "union", "variant"), CONSUMED)
@pytest.mark.spec("MEDULLA-CONTRACT-R005")
def test_a_consumed_variant_is_exported_where_its_union_is(
    module: str, union: str, variant: str
) -> None:
    surface = importlib.import_module(f"medulla.generated.{module}")
    models = importlib.import_module("medulla.generated.models")
    assert union in surface.__all__
    assert variant in surface.__all__
    assert variant in models.__all__
    assert getattr(surface, variant) is getattr(models, variant)


def _generated_method_alias():
    import ast
    import inspect
    from typing import get_args

    models = importlib.import_module("medulla.generated.models")
    tree = ast.parse(inspect.getsource(models))
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "Method"
            for target in node.targets
        )
    )
    variants = get_args(get_args(models.Method)[0])
    return assignment.value, [variant.__name__ for variant in variants]


@pytest.mark.spec("MEDULLA-CONTRACT-R001")
def test_generated_method_alias_has_bounded_ast_depth():
    import ast
    import math

    expression, names = _generated_method_alias()
    pending, maximum = [(expression, 1)], 0
    while pending:
        node, depth = pending.pop()
        maximum = max(maximum, depth)
        pending.extend((child, depth + 1) for child in ast.iter_child_nodes(node))
    assert maximum <= math.ceil(math.log2(len(names))) + 8
    ast.NodeVisitor().visit(expression)


@pytest.mark.spec("MEDULLA-CONTRACT-R001")
def test_generated_method_grouping_preserves_schema_and_validators():
    import ast
    import functools
    import operator
    from typing import Annotated, Any, Literal, get_args

    from pydantic import (
        AfterValidator,
        ConfigDict,
        Field,
        TypeAdapter,
        ValidationError,
        create_model,
    )

    expression, names = _generated_method_alias()

    def even(value: int) -> int:
        if value % 2:
            raise ValueError("even payload required")
        return value

    variants = []
    for name in names:
        # These runtime types deliberately depend on generated variant names.
        fields: dict[str, Any] = {
            "method": (Literal[name], ...),
            "payload": (
                Annotated[int, Field(strict=True, gt=0), AfterValidator(even)],
                ...,
            ),
        }
        variants.append(
            create_model(name, __config__=ConfigDict(extra="forbid"), **fields)
        )
    namespace = dict(zip(names, variants, strict=True))
    namespace.update(Annotated=Annotated, Field=Field)
    actual = eval(
        compile(ast.Expression(expression), "<generated Method>", "eval"), namespace
    )
    prior: Any = Annotated[
        functools.reduce(operator.or_, variants), Field(discriminator="method")
    ]
    assert get_args(get_args(actual)[0]) == tuple(variants)
    actual_adapter: TypeAdapter[Any] = TypeAdapter(actual)
    prior_adapter: TypeAdapter[Any] = TypeAdapter(prior)
    assert actual_adapter.json_schema() == prior_adapter.json_schema()
    for name, variant in zip(names, variants, strict=True):
        assert isinstance(
            actual_adapter.validate_python({"method": name, "payload": 2}), variant
        )
    for invalid in (
        {"method": "not-recorded", "payload": 2},
        {"method": names[0], "payload": 1},
        {"method": names[0], "payload": "2"},
        {"method": names[0], "payload": 0},
        {"method": names[0], "payload": 2, "extra": True},
    ):
        for adapter in (actual_adapter, prior_adapter):
            with pytest.raises(ValidationError):
                adapter.validate_python(invalid)
