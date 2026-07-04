"""Python client ⇄ Rust ``Method`` enum drift gate (CONCEPT:EG-KG.query.wire-protocol).

The Python↔Rust boundary is out-of-process MessagePack over UDS/TCP — there is
**no PyO3 / FFI**, so nothing generates or type-checks the Python client against
the engine. The wire contract is the ``Method`` enum in
``crates/eg-types/src/protocol.rs`` (a serde-tagged enum, ``tag = "method"``),
which the hand-written Python client mirrors by sending the variant name as a
string: ``self._client._send("AddNode", {...})``.

Nothing kept the two in lockstep — a renamed/removed Rust variant, or a typo in a
Python ``_send`` string, would only surface as a runtime error in production.
This gate closes that gap with two assertions:

1. **No drift (hard):** every method name the Python client sends MUST be a real
   ``Method`` variant. Catches typos and methods the engine dropped/renamed.
2. **Binding ratchet:** the set of variants with no Python sender must equal the
   committed baseline. Adding a new engine op forces a conscious choice — bind it
   in the client, or record it in ``protocol_unbound_baseline.txt`` with a reason.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parent.parent
_PROTOCOL = _ROOT / "crates" / "eg-types" / "src" / "protocol.rs"
_CLIENT = _ROOT / "epistemic_graph" / "client.py"
_BASELINE = Path(__file__).parent / "protocol_unbound_baseline.txt"


def _rust_method_variants() -> set[str]:
    """Top-level variant identifiers of the ``pub enum Method { ... }`` block."""
    text = _PROTOCOL.read_text(encoding="utf-8")
    m = re.search(r"pub enum Method\s*\{", text)
    assert m, "Could not locate `pub enum Method {` in protocol.rs"
    start = m.end()
    depth, i = 1, start
    while i < len(text) and depth:
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
        i += 1
    body = text[start : i - 1]
    variants: set[str] = set()
    for line in body.splitlines():
        # Variants sit at exactly 4-space indent; struct fields are deeper, and
        # attributes/doc-comments begin with '#' or '/'.
        vm = re.match(r"^    ([A-Z][A-Za-z0-9]*)\b", line)
        if vm:
            variants.add(vm.group(1))
    return variants


def _python_sent_methods() -> set[str]:
    """Method-name string literals the client passes to ``_send(...)``."""
    text = _CLIENT.read_text(encoding="utf-8")
    # ``\s*`` spans newlines (multi-line call sites), so this catches both
    # ``_send("X"`` and ``_send(\n    "X"``.
    return set(re.findall(r'_send\(\s*"([A-Za-z0-9_]+)"', text))


def _baseline() -> set[str]:
    return {
        line.strip()
        for line in _BASELINE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def test_parser_finds_the_contract():
    # Guard against a silently-broken parser declaring a falsely-clean gate.
    variants = _rust_method_variants()
    assert len(variants) > 100, f"Only parsed {len(variants)} Method variants"
    sent = _python_sent_methods()
    assert len(sent) > 50, f"Only parsed {len(sent)} client _send calls"


def test_no_python_client_drift():
    variants = _rust_method_variants()
    sent = _python_sent_methods()
    drift = sorted(sent - variants)
    assert not drift, (
        "Python client sends method names with no matching Rust `Method` "
        f"variant (renamed/removed in the engine, or a typo): {drift}"
    )


def test_unbound_variants_match_baseline():
    variants = _rust_method_variants()
    sent = _python_sent_methods()
    unbound = variants - sent
    baseline = _baseline()
    newly_unbound = sorted(unbound - baseline)
    assert not newly_unbound, (
        "New Rust `Method` variants have no Python client binding: "
        f"{newly_unbound}. Bind them in epistemic_graph/client.py, or add them "
        "to tests/protocol_unbound_baseline.txt with a reason."
    )
    stale = sorted(baseline - unbound)
    assert not stale, (
        "Baseline lists variants that are now bound (or no longer exist) — "
        f"remove them from protocol_unbound_baseline.txt: {stale}"
    )


@pytest.mark.skip(reason="diagnostic helper, run manually")
def test_print_contract_summary():  # pragma: no cover
    print("variants:", len(_rust_method_variants()))
    print("sent:", len(_python_sent_methods()))
