"""The terms of acceptance for the cyclomatic cap, exercised on known shapes.

Every case here is a shape the rule must get right, not a sample of what the
tree happens to contain today. A synthetic long dispatcher preserves the
regression for the old 400-line scan window. The two real-tree cases at the end
keep both sides of the rule connected to current dispatchers:
`streaming.rs::try_handle` ends in a catch-all and must not be exempt, while
`wire/mod.rs::dispatch_kind` is exhaustive and may be exempt.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine

CAP_CYCLOMATIC = 10
CAP_COGNITIVE = 15


def _module():
    path = ROOT / "scripts" / "rust_exhaustive_match.py"
    spec = importlib.util.spec_from_file_location("rust_exhaustive_match", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["rust_exhaustive_match"] = module
    spec.loader.exec_module(module)
    return module


def _shape(source: str):
    return _module().dispatch_shape(source, 1)


def test_flat_exhaustive_match_has_no_catch_all():
    source = """fn dispatch(kind: Kind) -> u8 {
    match kind {
        Kind::A => 1,
        Kind::B => 2,
        Kind::C => 3,
    }
}
"""
    assert _shape(source) == (3, 0)


@pytest.mark.parametrize(
    "arm",
    [
        "_ => 0",
        "other => 0",
        "ref rest => 0",
        "mut rest => 0",
        "bound @ _ => 0",
        "n if n > 2 => 0",
        "Kind::A | leftover => 0",
    ],
)
def test_every_irrefutable_arm_shape_is_a_catch_all(arm):
    source = f"""fn dispatch(kind: Kind) -> u8 {{
    match kind {{
        Kind::A => 1,
        {arm},
    }}
}}
"""
    shape = _shape(source)
    assert shape is not None and shape.catch_alls == 1


@pytest.mark.parametrize(
    "arm",
    [
        "Kind::B => 2",
        "Kind::B | Kind::C => 2",
        "Kind::B { field } => 2",
        "Kind::B(inner) => 2",
        "kind::lowercase::Path => 2",
        "MAX_LIMIT => 2",
    ],
)
def test_discriminating_arms_are_not_catch_alls(arm):
    source = f"""fn dispatch(kind: Kind) -> u8 {{
    match kind {{
        Kind::A => 1,
        {arm},
    }}
}}
"""
    shape = _shape(source)
    assert shape is not None and shape.catch_alls == 0


def test_a_catch_all_in_a_string_or_comment_is_not_an_arm():
    source = """fn dispatch(kind: Kind) -> u8 {
    // fallback shape: _ => 0
    let note = "_ => 0";
    match kind {
        Kind::A => 1,
        Kind::B => 2,
    }
}
"""
    assert _shape(source) == (2, 0)


def test_a_nested_match_is_counted_and_its_catch_all_disqualifies():
    source = """fn dispatch(kind: Kind, sub: Sub) -> u8 {
    match kind {
        Kind::A => match sub {
            Sub::X => 1,
            _ => 2,
        },
        Kind::B => 3,
    }
}
"""
    assert _shape(source) == (4, 1)


def test_a_function_without_any_match_is_never_exempt():
    """Branching that is not dispatch gets no relief, whatever its shape."""
    module = _module()
    ladder = (
        "fn f(x: u32) -> u32 {\n"
        + "".join(f"    if x == {index} {{ return {index}; }}\n" for index in range(20))
        + "    0\n}\n"
    )
    assert module.dispatch_shape(ladder, 1) == (0, 0)
    assert not module.exhaustive_dispatch_exempt(
        ladder, 1, 21, 2, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def test_non_rust_source_is_never_exempt():
    module = _module()
    assert not module.exhaustive_dispatch_exempt(
        None, 1, 30, 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def test_cognitive_complexity_is_never_exempt():
    module = _module()
    source = """fn dispatch(kind: Kind) -> u8 {
    match kind {
        Kind::A => 1,
        Kind::B => 2,
    }
}
"""
    assert not module.exhaustive_dispatch_exempt(
        source, 1, 30, CAP_COGNITIVE + 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def test_residual_branching_beyond_the_cap_is_never_exempt():
    """Arms are discounted; everything else still faces the ordinary cap."""
    module = _module()
    source = """fn dispatch(kind: Kind) -> u8 {
    match kind {
        Kind::A => 1,
        Kind::B => 2,
    }
}
"""
    # 2 arms discounted from 13 leaves a residual of 11: over the cap of 10.
    assert not module.exhaustive_dispatch_exempt(
        source, 1, 13, 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )
    # 2 arms discounted from 12 leaves 10: exactly at the cap, accepted.
    assert module.exhaustive_dispatch_exempt(
        source, 1, 12, 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def test_an_arm_count_above_the_measured_cyclomatic_is_never_exempt():
    """An unprovable attribution must fail closed, not round in our favour."""
    module = _module()
    arms = "".join(f"        Kind::V{index} => {index},\n" for index in range(12))
    source = f"fn dispatch(kind: Kind) -> u8 {{\n    match kind {{\n{arms}    }}\n}}\n"
    assert module.dispatch_shape(source, 1) == (12, 0)
    # 12 arms cannot be attributed inside a measured cyclomatic of 11.
    assert not module.exhaustive_dispatch_exempt(
        source, 1, 11, 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def test_a_trailing_catch_all_beyond_400_lines_is_not_exempt():
    """Brace matching must reach a catch-all beyond the old scan window."""
    module = _module()
    padding = "".join(f"        // spacer {index}\n" for index in range(450))
    source = f"""fn dispatch(kind: Kind) -> u8 {{
    match kind {{
        Kind::A => 1,
{padding}        other => 0,
    }}
}}
"""
    assert source[: source.index("other =>")].count("\n") > 400
    assert module.dispatch_shape(source, 1) == (2, 1)
    assert not module.exhaustive_dispatch_exempt(
        source, 1, CAP_CYCLOMATIC + 1, 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def _function_line(path: Path, name: str) -> int:
    text = path.read_text(encoding="utf-8")
    match = re.search(
        rf"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?"
        rf"(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?fn {name}\b",
        text,
        re.M,
    )
    assert match is not None, f"{name} no longer exists in {path}"
    return text.count("\n", 0, match.start()) + 1


def test_real_tree_dispatcher_with_a_trailing_catch_all_is_not_exempt():
    """`streaming.rs::try_handle`: cyclomatic 12, cognitive 3, trailing catch-all.

    The ten match arms leave a residual cyclomatic complexity of two, so this
    function would qualify for the exhaustive-dispatch exemption if the final
    `other => Err(other)` arm were missed.
    """
    module = _module()
    path = ROOT / "src" / "server" / "handlers" / "streaming.rs"
    line = _function_line(path, "try_handle")
    source = path.read_text(encoding="utf-8")

    shape = module.dispatch_shape(source, line)
    assert shape == (10, 1)
    assert 12 - shape.arms == 2
    assert not module.exhaustive_dispatch_exempt(
        source, line, 12, 3, CAP_CYCLOMATIC, CAP_COGNITIVE
    )


def test_real_tree_exhaustive_dispatcher_is_exempt():
    """`wire/mod.rs::dispatch_kind`: cyclomatic 32, cognitive 1, no catch-all."""
    module = _module()
    path = ROOT / "src" / "server" / "wire" / "mod.rs"
    line = _function_line(path, "dispatch_kind")
    source = path.read_text(encoding="utf-8")

    shape = module.dispatch_shape(source, line)
    assert shape is not None and shape.arms > CAP_CYCLOMATIC
    assert shape.catch_alls == 0
    assert module.exhaustive_dispatch_exempt(
        source, line, shape.arms + 1, 1, CAP_CYCLOMATIC, CAP_COGNITIVE
    )
