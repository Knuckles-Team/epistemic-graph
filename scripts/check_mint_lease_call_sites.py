#!/usr/bin/env python3
"""Fail CI when `mint_policy_decision_lease` grows a second production call site.

GRAPH-POLICY-LEASE-CONTRACT.md §3 hardening (`MintAuthorization`,
`crates/eg-core/src/isolation.rs`) closes the caller-supplied-claims gap by
requiring a cryptographic capability token instead of a raw
`RequestContextClaims` reference. That control is only as good as the claim
that exactly one production code path ever calls
`IsolationLayer::mint_policy_decision_lease`, and that the path in question
already holds the server auth secret at the point it has verified claims in
hand. A second production call site (a future internal tool, an admin surface,
a copy-pasted handler) would be a NEW place that must independently get the
`MintAuthorization` construction right, and nothing else in the build would
notice if it didn't. This gate is the notice.

The gate used to pin that path to one literal file, `src/server/dispatch.rs`.
The dispatch decomposition moved the caller into
`src/server/dispatch/router.rs` (the `authorize_and_route_knowledge_stream`
body now lives inside `dispatch_governed_stream_write_methods`) without
changing a line of the authorization it performs, and the gate reported that
pure relocation as a policy violation. A file path was never the invariant.
What is checked now instead: the sole production caller lives in the
compiler-declared `src/server/dispatch` module tree — the one subsystem that
owns request authorization and routing — and the function containing it
constructs its `MintAuthorization` from the server auth secret and the
verified claims. That is the property the audit actually rests on, and unlike
a path it survives the next decomposition while still failing closed on a
caller that appears anywhere else or that skips the MAC construction.

Test-only call sites are expected and permitted (see `_is_test_only` below) —
this is a call-site-count gate, not a ban on exercising the mint path in
tests.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_module_tree import read_module_paths  # noqa: E402

# The one and only subsystem permitted to hold the production call site, named
# by its module root rather than by a leaf filename.
ALLOWED_PRODUCTION_CALL_MODULE = "src/server/dispatch.rs"

# What the audit of that call site actually rests on: the enclosing function
# builds the capability token itself, from the server auth secret and the
# verified claims it already holds, rather than accepting one from a caller.
REQUIRED_MINT_AUTHORIZATION_TOKENS = (
    "let (auth_secret, isolation)",
    "MintAuthorization::compute_mac(",
    "MintAuthorization::new(",
    "auth_secret",
    "verified_context.claims()",
    "CarrierAuthority::from_verified(",
    "AccessLevel::Read",
    "KnowledgeStreamAuthority::from_verified_with_lease(",
    "policy_store()",
)

# A `fn` item at ANY nesting depth. Rust puts most real code in `impl`/`mod`/
# `trait` blocks, so anchoring this at column zero (as it once was) makes an
# `impl` method invisible: the enclosing-function lookup then either falls back
# to the whole file or attributes the call to an unrelated free function that
# happens to sit above it. Both outcomes let a neighbour's tokens satisfy the
# audit for a call site that never constructs its own `MintAuthorization`.
_ITEM_FN = re.compile(
    r"^[ \t]*(?:pub(?:\([^)]*\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?"
    r'(?:unsafe\s+)?(?:extern\s+"[^"]*"\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)',
    re.MULTILINE,
)

# A real method call: `<receiver>.mint_policy_decision_lease(`. This deliberately
# does NOT match the method's own `pub fn mint_policy_decision_lease(` definition
# (no leading `.`) nor a doc/prose reference like
# "[`IsolationLayer::mint_policy_decision_lease`]" (`::`, not `.`, and no `(`
# immediately after the identifier in that form).
_CALL_SITE = re.compile(r"\.mint_policy_decision_lease\s*\(")

# Returned when no `fn` item encloses the call site. The gate fails on it: an
# unattributable call is exactly the case where reading the whole file would
# hand the audit a neighbour's tokens.
NO_ENCLOSING_FUNCTION = "<no enclosing fn>"

# Directories this repo's own Rust sources live under (mirrors what `cargo
# check -p epistemic-graph` / `-p eg-core` actually compile — see
# `scripts/check_current_only_architecture.py` for the same read-the-shipped-
# tree convention this gate follows).
_SCAN_DIRS = ("src", "crates")

# Path-shape conventions this repo uses for test-only Rust source (mirrors
# what `#[cfg(test)]` actually gates in practice): a `tests.rs` leaf module
# (e.g. `src/server/handlers/knowledge_stream/tests.rs`, wired behind
# `#[cfg(test)] mod tests;` in its parent `mod.rs`), anything under a `tests/`
# directory (unit-test submodules or the top-level integration-test crate),
# or a `..._test(s).rs` / `test_...rs` leaf filename.
_TEST_ONLY_PATH = re.compile(
    r"(^|/)(tests?)/.*\.rs$"  # under a tests/ or test/ directory
    r"|(^|/)tests\.rs$"  # a `tests.rs` leaf module
    r"|(^|/)test_[^/]*\.rs$"  # test_*.rs
    r"|(^|/)[^/]*_tests?\.rs$"  # *_test.rs / *_tests.rs
)


def _is_test_only(relative_posix: str) -> bool:
    return bool(_TEST_ONLY_PATH.search(relative_posix))


def _is_commented_out(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("//") or stripped.startswith("*")


def find_call_sites(root: Path) -> list[tuple[str, int]]:
    """Every real `.mint_policy_decision_lease(` call under `root`'s Rust sources.

    Returns `(path-relative-to-root-as-posix, 1-based line number)` pairs,
    sorted for deterministic output.
    """

    sites: list[tuple[str, int]] = []
    for scan_dir in _SCAN_DIRS:
        base = root / scan_dir
        if not base.is_dir():
            continue
        for rust_file in sorted(base.rglob("*.rs")):
            relative = rust_file.relative_to(root).as_posix()
            text = rust_file.read_text(encoding="utf-8")
            for line_no, line in enumerate(text.splitlines(), start=1):
                if _CALL_SITE.search(line) and not _is_commented_out(line):
                    sites.append((relative, line_no))
    return sorted(sites)


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _body_opening(lines: list[str], declaration: int) -> int | None:
    """Index of the line that opens the `fn` body, or `None` if it has none.

    A signature can span several lines and its continuation lines (`) -> T {`,
    a `where` clause) are not indented deeper than the declaration, so the body
    boundary is found from the opening brace rather than from indentation
    alone. A trait method declared without a body ends in `;` and has no span.
    """

    for index in range(declaration, len(lines)):
        stripped = lines[index].rstrip()
        if "{" in stripped:
            return index
        if stripped.endswith(";"):
            return None
    return None


def _function_span(lines: list[str], declaration: int, indent: int) -> int:
    """Exclusive end line of the `fn` declared at `declaration`.

    `rustfmt` closes a function with a `}` at exactly the declaration's
    indentation, so the first non-blank line after the opening brace that is
    not indented deeper ends the body. A source line that starts at a
    shallower column inside the body (only reachable from a multi-line raw
    string literal) ends the window early, which narrows the audited text and
    can only make this gate stricter.
    """

    opening = _body_opening(lines, declaration)
    if opening is None:
        return declaration + 1
    if lines[opening].rstrip().endswith("}"):
        return opening + 1
    for index in range(opening + 1, len(lines)):
        line = lines[index]
        if line.strip() and _indent(line) <= indent:
            return index + 1
    return len(lines)


def enclosing_function(text: str, line_no: int) -> tuple[str, str]:
    """The `fn` item containing 1-based `line_no`, as `(name, body)`.

    `fn` items are recognised at any nesting depth -- an `impl` method, a
    method inside a nested `mod`, a trait default body -- and each candidate's
    window is its own indentation-delimited body, so a call can never be
    attributed to a neighbouring item. When several `fn` items contain the call
    (an inner helper declared inside a function body) the OUTERMOST one wins:
    the audited window must never shrink below the function the reviewer read.
    `NO_ENCLOSING_FUNCTION` is returned when no `fn` contains the call, and the
    audit treats that as a failure rather than reading the whole file.
    """

    lines = text.splitlines()
    target = line_no - 1
    containing = []
    for index, line in enumerate(lines):
        match = _ITEM_FN.match(line)
        if match is None:
            continue
        end = _function_span(lines, index, _indent(line))
        if index <= target < end:
            containing.append((index, end, match.group(1)))
    if not containing:
        return NO_ENCLOSING_FUNCTION, ""
    start, end, name = min(containing)
    return name, "\n".join(lines[start:end]) + "\n"


def require_sole_owning_call_site(
    root: Path, production: list[tuple[str, int]]
) -> tuple[str, int]:
    """The single production call site, proven to live in the owning module.

    The module is named by its root (`ALLOWED_PRODUCTION_CALL_MODULE`) and
    expanded through the compiler-declared `mod` graph, so decomposing the
    dispatch subsystem into more files never reads as a relocated caller, while
    a caller appearing in any OTHER subsystem still fails closed.
    """

    if len(production) != 1:
        rendered = ", ".join(f"{path}:{line}" for path, line in production)
        raise SystemExit(
            "mint-lease call-site gate failed: expected exactly ONE production "
            f"call site (in the {ALLOWED_PRODUCTION_CALL_MODULE} module), found "
            f"{len(production)}: [{rendered or 'none'}]. A second production "
            "caller of mint_policy_decision_lease means a second place that must "
            "independently get MintAuthorization construction right — audit it "
            "before letting this pass."
        )
    only_path, only_line = production[0]
    owning_module = {
        path.relative_to(root).as_posix()
        for path in read_module_paths(
            ALLOWED_PRODUCTION_CALL_MODULE, root, include_tests=True
        )
    }
    if only_path not in owning_module:
        raise SystemExit(
            "mint-lease call-site gate failed: the sole production call site is "
            f"{only_path}:{only_line}, outside the "
            f"{ALLOWED_PRODUCTION_CALL_MODULE} module that owns request "
            "authorization. Widening the allowed module is a policy decision, "
            "not a path fix: the new owner must independently hold the server "
            "auth secret and the verified claims at the call."
        )
    return only_path, only_line


def require_self_constructed_authorization(
    root: Path, relative: str, line_no: int
) -> str:
    """Prove the call site builds its own `MintAuthorization`; return its `fn`.

    This is the property the §3 audit rests on -- a caller-supplied capability
    token is precisely the gap it closes -- so it, not a file path, is what the
    gate pins.
    """

    function, body = enclosing_function(
        (root / relative).read_text(encoding="utf-8"), line_no
    )
    if function == NO_ENCLOSING_FUNCTION:
        raise SystemExit(
            "mint-lease call-site gate failed: the sole production call site "
            f"{relative}:{line_no} is not inside any `fn` item, so there is no "
            "function whose MintAuthorization construction can be audited."
        )
    missing = [
        token for token in REQUIRED_MINT_AUTHORIZATION_TOKENS if token not in body
    ]
    if missing:
        raise SystemExit(
            "mint-lease call-site gate failed: the sole production call site "
            f"{relative}:{line_no} (in {function}) does not construct its own "
            f"MintAuthorization — missing {missing}. A caller-supplied capability "
            "token is exactly the gap GRAPH-POLICY-LEASE-CONTRACT.md §3 closes."
        )
    return function


def check(root: Path) -> None:
    sites = find_call_sites(root)
    if not sites:
        raise SystemExit(
            "mint-lease call-site gate failed: found ZERO calls to "
            "mint_policy_decision_lease — either the scan paths drifted or the "
            "feature was deleted; update this gate deliberately if so, don't "
            "let it go quietly blind"
        )

    production = [(path, line) for path, line in sites if not _is_test_only(path)]
    test_only = [(path, line) for path, line in sites if _is_test_only(path)]

    only_path, only_line = require_sole_owning_call_site(root, production)
    function = require_self_constructed_authorization(root, only_path, only_line)

    print(
        "mint-lease call-site gate passed: "
        f"1 production call site ({only_path}:{only_line} in {function}), "
        f"{len(test_only)} test-only call site(s)"
    )


def main() -> None:
    check(ROOT)


if __name__ == "__main__":
    main()
