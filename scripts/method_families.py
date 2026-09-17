"""Resolve `Method` write families for the static architecture gates.

Classifiers select the named families declared once next to the enum
(`crates/eg-types/src/protocol/method/families.rs`: one
`MethodWriteFamily::X` result arm per family inside `Method::write_family`).
A gate that inventories a classifier's variants textually must see the variants
a family reference contributes. They are read from code only: comments and
literals are masked, so a variant removed from a family arm is missing from
every classifier that selects that family, and a commented-out variant never
counts.
"""

from __future__ import annotations

import re
from pathlib import Path

from rust_lexer import _balanced_span_from, _rust_code_mask

FAMILIES_PATH = "crates/eg-types/src/protocol/method/families.rs"
_FAMILY_REFERENCE = re.compile(r"\bMethodWriteFamily\s*::\s*([A-Z][A-Za-z0-9_]*)")
_ARM_RESULT = re.compile(
    r"=>\s*(?:\{\s*)?MethodWriteFamily\s*::\s*([A-Z][A-Za-z0-9_]*)\s*\}?\s*,?"
)


def families_source(root: Path) -> str:
    return (root / FAMILIES_PATH).read_text(encoding="utf-8")


def family_pattern(families: str, name: str) -> str | None:
    """The code-only variant pattern of family `name`'s arm, `Self::` read as
    `Method::`, or `None` when `write_family` has no arm for that family."""

    return _family_patterns(families).get(name)


def _family_patterns(families: str) -> dict[str, str]:
    mask = _rust_code_mask(families)
    function = re.search(r"\bfn\s+write_family\s*\(", mask)
    if function is None:
        return {}
    start = mask.find("{", function.end())
    end = _balanced_span_from(mask, start, "{", "}")
    body = mask[start + 1 : end]
    opener = re.search(r"\bmatch\s+self\s*\{", body)
    if opener is None:
        return {}
    patterns: dict[str, str] = {}
    cursor = opener.end()
    for arm in _ARM_RESULT.finditer(body, cursor):
        pattern = body[cursor : arm.start()].replace("Self::", "Method::")
        patterns[arm.group(1)] = patterns.get(arm.group(1), "") + pattern
        cursor = arm.end()
    return patterns


def expand_method_families(block: str, families: str) -> str:
    """`block` plus the code-only variant patterns of every family it names."""

    expanded = [block]
    patterns = _family_patterns(families)
    for name in sorted(set(_FAMILY_REFERENCE.findall(_rust_code_mask(block)))):
        if name in patterns:
            expanded.append(patterns[name])
    return "\n".join(expanded)
