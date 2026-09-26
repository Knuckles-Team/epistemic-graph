"""Engine-owned deterministic ETL record transformation contracts."""

from __future__ import annotations

import pytest

from epistemic_graph.etl_transforms import (
    cast,
    coalesce,
    dig,
    flatten,
    rename,
    stable_id,
)

pytestmark = pytest.mark.no_engine


def test_dig_resolves_dotted_path():
    record = {"attributes": {"name": "Checking"}}
    assert dig(record, "attributes.name") == "Checking"
    assert dig(record, "attributes.missing") is None
    assert dig(record, "attributes.missing", default="fallback") == "fallback"


def test_dig_non_dict_returns_default():
    assert dig(None, "a.b") is None
    assert dig("not-a-dict", "a.b", default=0) == 0  # type: ignore[arg-type]


def test_coalesce_falls_through_falsy_like_or():
    record = {"name": "", "title": None, "id": 7}
    assert coalesce(record, "name", "title", default="Untitled") == "Untitled"
    assert coalesce(record, "name", "id", default="Untitled") == 7


def test_coalesce_dotted_paths():
    record = {"attributes": {"group_title": "Groceries"}}
    assert (
        coalesce(record, "attributes.group_title", "attributes.description")
        == "Groceries"
    )


def test_stable_id_plain_concatenation():
    assert stable_id("acme", prefix="dockerhub") == "dockerhub:acme"
    assert stable_id(42, prefix="firefly:account") == "firefly:account:42"
    assert stable_id("a", "b", "c") == "a:b:c"
    # None/empty parts are dropped, not embedded as literal "None"
    assert stable_id(None, "b", "") == "b"


def test_stable_id_hashed_variant_is_deterministic():
    first = stable_id("a long free-text body", hash_algo="sha1")
    second = stable_id("a long free-text body", hash_algo="sha1")
    assert first == second
    assert first != stable_id("a different body", hash_algo="sha1")


def test_cast_best_effort_never_raises():
    assert cast("42", int) == 42
    assert cast(None, int) is None
    assert cast(None, int, default=0) == 0
    assert cast("not-a-number", int, default=-1) == -1
    assert cast("true", bool) is True
    assert cast("0", bool) is False
    assert cast("no", bool) is False


def test_rename_maps_top_level_keys():
    out = rename({"a": 1, "b": 2}, {"a": "x"})
    assert out == {"x": 1, "b": 2}


def test_flatten_nested_dict():
    record = {"attributes": {"name": "A", "nested": {"deep": 1}}, "id": "5"}
    flat = flatten(record)
    assert flat["attributes.name"] == "A"
    assert flat["attributes.nested.deep"] == 1
    assert flat["id"] == "5"


def test_flatten_keeps_lists_as_is():
    record = {"tags": [1, 2, 3]}
    assert flatten(record)["tags"] == [1, 2, 3]
