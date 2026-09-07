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
    "MintAuthorization::compute_mac(",
    "MintAuthorization::new(",
    "auth_secret",
    ".claims()",
)

_ITEM_FN = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)

# A real method call: `<receiver>.mint_policy_decision_lease(`. This deliberately
# does NOT match the method's own `pub fn mint_policy_decision_lease(` definition
# (no leading `.`) nor a doc/prose reference like
# "[`IsolationLayer::mint_policy_decision_lease`]" (`::`, not `.`, and no `(`
# immediately after the identifier in that form).
_CALL_SITE = re.compile(r"\.mint_policy_decision_lease\s*\(")

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


def enclosing_function(text: str, line_no: int) -> tuple[str, str]:
    """The item-level `fn` containing 1-based `line_no`, as `(name, body)`.

    Only column-zero `fn` items are considered, so a nested closure or an inner
    helper cannot shrink the audited window below the function the reviewer
    actually read.
    """

    offset = sum(len(line) + 1 for line in text.splitlines()[: line_no - 1])
    declarations = [match for match in _ITEM_FN.finditer(text)]
    enclosing = [match for match in declarations if match.start() <= offset]
    if not enclosing:
        return "<file>", text
    start = enclosing[-1]
    following = next(
        (match for match in declarations if match.start() > start.start()), None
    )
    end = following.start() if following else len(text)
    return start.group(1), text[start.start() : end]


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
