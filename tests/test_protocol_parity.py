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

import ast
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

# Purely static: reads/regexes protocol.rs + client.py as text, never builds
# or connects to the shared native engine (found while validating W2.5 --
# this file's own session paid the full cargo-build+server-boot fixture cost
# for zero benefit).
pytestmark = pytest.mark.no_engine

_ROOT = Path(__file__).parent.parent
_PROTOCOL = _ROOT / "crates" / "eg-types" / "src" / "protocol.rs"
_CLIENT = _ROOT / "epistemic_graph" / "client.py"
_KNOWLEDGE_STREAM = _ROOT / "crates" / "eg-types" / "src" / "knowledge_stream.rs"
_BASELINE = Path(__file__).parent / "protocol_unbound_baseline.txt"

_RETIRED_METHODS = {
    "BatchCosineSimilarity",
    "SpectralCluster",
    "HypergraphEncodeInteraction",
    "FindSimilarPairs",
}


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


def _send_forwarding_parameters(tree: ast.AST) -> dict[str, int]:
    """Map ``helper name -> index of the parameter it forwards to ``_send``.

    Some client methods do not call ``_send`` with a literal; they hand the
    method name to a shared validator that sends it (``_mutate("RenewCapacity",
    ...)``, ``_reclaim_or_status("CapacityStatus", ...)``). A regex over
    ``_send("..."`` cannot see those, and reported five genuinely-bound capacity
    methods as unbound — a gate that under-reports coverage pushes real bindings
    into the deferral baseline, which is the opposite of what this ratchet is
    for. One hop of parameter flow, resolved below, is what the shape actually
    needs.
    """

    forwarding: dict[str, int] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        params = [arg.arg for arg in node.args.args] + [
            arg.arg for arg in node.args.kwonlyargs
        ]
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call) or not inner.args:
                continue
            func = inner.func
            if not (isinstance(func, ast.Attribute) and func.attr == "_send"):
                continue
            first = inner.args[0]
            if isinstance(first, ast.Name) and first.id in params:
                forwarding[node.name] = params.index(first.id)
    return forwarding


def _python_sent_methods() -> set[str]:
    """Method names the client can actually put on the wire.

    Direct ``_send("X", ...)`` literals, plus literals handed to a helper that
    forwards its own parameter to ``_send`` (see
    :func:`_send_forwarding_parameters`). Both reach the engine identically, so
    both count as bound.
    """

    text = _CLIENT.read_text(encoding="utf-8")
    # ``\s*`` spans newlines (multi-line call sites), so this catches both
    # ``_send("X"`` and ``_send(\n    "X"``.
    sent = set(re.findall(r'_send\(\s*"([A-Za-z0-9_]+)"', text))

    tree = ast.parse(text)
    forwarding = _send_forwarding_parameters(tree)
    # Resolve to a fixpoint so a helper that forwards into another helper is
    # still followed, rather than silently stopping after one hop.
    while True:
        grew = False
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else func.id if isinstance(func, ast.Name) else None
            )
            index = forwarding.get(name) if name else None
            if index is None:
                continue
            # `self` is not in a call's positional args but IS in the def's
            # parameter list, so a bound-method call site is one short.
            position = index - 1 if isinstance(func, ast.Attribute) else index
            if 0 <= position < len(node.args):
                argument = node.args[position]
                if isinstance(argument, ast.Constant) and isinstance(
                    argument.value, str
                ):
                    grew |= argument.value not in sent
                    sent.add(argument.value)
        if not grew:
            return sent


@dataclass(frozen=True)
class _BaselineEntry:
    """One parsed line of ``protocol_unbound_baseline.txt``.

    ``review_by`` is set for a time-boxed deferral; ``permanent`` marks a
    genuinely engine-internal/maintenance op exempt from the date check.
    Exactly one of the two must be present — enforced by
    ``test_baseline_entries_well_formed`` — so a NEW line with neither can't
    silently join the ratchet with no expiry (the defect this format fixes:
    entries carried a reason but no owner and no date, so "temporarily
    deferred" silently became permanent).
    """

    name: str
    line_no: int
    owner: str | None
    review_by: date | None
    permanent: bool
    reason: str | None  # required, non-empty, when permanent=True


# `<Method>  # owner=<@handle> review-by=<YYYY-MM-DD> [note=...]`
# `<Method>  # owner=<@handle> PERMANENT reason=<text>`
#
# Token-based (not one rigid whole-comment regex) so a metadata comment with
# a field MISSING or MISSPELLED still parses — as an entry with that field
# `None`/`False` — rather than the whole line failing to match and raising at
# parse time. Malformed-but-present metadata is then a clear, specific
# failure in `test_baseline_entries_well_formed` ("owner is missing") instead
# of an opaque regex-didn't-match assertion; that keeps the two concerns
# (syntax vs completeness) in the test whose job it is to report each.
_NAME_RE = re.compile(r"^(?P<name>[A-Za-z][A-Za-z0-9_]*)\s*(?:#\s*(?P<meta>.*))?$")
_OWNER_TOKEN_RE = re.compile(r"\bowner=(\S+)")
_REVIEW_BY_TOKEN_RE = re.compile(r"\breview-by=(\d{4}-\d{2}-\d{2})\b")
_PERMANENT_TOKEN_RE = re.compile(r"\bPERMANENT\b")
_REASON_TOKEN_RE = re.compile(r"\breason=(.+)$")


def _parse_entries(text: str) -> list[_BaselineEntry]:
    """Parse baseline-file TEXT into entries — pure, no file I/O.

    Split out from :func:`_baseline_entries` so the review-date gate can be
    proven against fabricated in-memory text (both a stale and a fresh
    entry) without waiting on the wall clock or mutating the real baseline.
    """
    entries: list[_BaselineEntry] = []
    for line_no, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        # A pure comment line (the block explanations above each group) has no
        # bare leading identifier; a method line always starts with one, with
        # its metadata (if any) trailing after a literal ``#``.
        if not line or line.startswith("#"):
            continue
        m = _NAME_RE.match(line)
        assert m, (
            f"tests/protocol_unbound_baseline.txt:{line_no}: unparsable entry "
            f"line {raw!r} — expected `<Method>` optionally followed by a "
            "`# ...` metadata comment"
        )
        meta = m.group("meta") or ""
        owner_m = _OWNER_TOKEN_RE.search(meta)
        review_by_m = _REVIEW_BY_TOKEN_RE.search(meta)
        reason_m = _REASON_TOKEN_RE.search(meta)
        entries.append(
            _BaselineEntry(
                name=m.group("name"),
                line_no=line_no,
                owner=owner_m.group(1) if owner_m else None,
                review_by=date.fromisoformat(review_by_m.group(1))
                if review_by_m
                else None,
                permanent=bool(_PERMANENT_TOKEN_RE.search(meta)),
                reason=reason_m.group(1).strip() if reason_m else None,
            )
        )
    return entries


def _baseline_entries() -> list[_BaselineEntry]:
    return _parse_entries(_BASELINE.read_text(encoding="utf-8"))


def _baseline() -> set[str]:
    return {entry.name for entry in _baseline_entries()}


def _is_well_formed(entry: _BaselineEntry) -> bool:
    """An entry must have an owner AND (a review-by date OR a justified
    PERMANENT marker) — checked as one shared predicate so the real gate
    (``test_baseline_entries_well_formed``) and its proof
    (``test_baseline_entries_reject_missing_metadata_fields``) can never
    silently drift onto two different definitions of "well-formed".
    """
    if not entry.owner:
        return False
    if entry.review_by is not None:
        return True
    return entry.permanent and bool((entry.reason or "").strip())


def _stale_entries(
    entries: list[_BaselineEntry], as_of: date
) -> list[_BaselineEntry]:
    """Time-boxed entries whose ``review_by`` has passed as of ``as_of``.

    Pure function of its inputs (no wall-clock read) so it can be exercised
    both by the real gate (called with ``date.today()``) and by a
    self-contained proof test that fabricates entries on both sides of a
    fixed ``as_of`` — see ``test_review_date_gate_catches_stale_and_honors_fresh``.
    """
    return [
        entry
        for entry in entries
        if not entry.permanent and entry.review_by is not None and entry.review_by < as_of
    ]


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


def test_retired_protocol_surfaces_are_absent():
    variants = _rust_method_variants()
    sent = _python_sent_methods()
    assert not (_RETIRED_METHODS & variants)
    assert not (_RETIRED_METHODS & sent)

    protocol = _PROTOCOL.read_text(encoding="utf-8")
    protocol_contract = protocol.split("// ── Tests", maxsplit=1)[0]
    client = _CLIENT.read_text(encoding="utf-8")
    stream = _KNOWLEDGE_STREAM.read_text(encoding="utf-8")
    assert "reorder_filter_selectivity" not in protocol_contract
    assert 'content = "params", deny_unknown_fields' in protocol_contract
    assert "reorder_filter_selectivity" not in client
    assert "CompatibilityMsgpackV1" not in stream
    assert "ArrowIpcV1" in stream


def test_baseline_entries_well_formed():
    """Every baseline entry carries an owner and EITHER a review-by date OR an
    explicit, justified PERMANENT marker — never neither.

    This is the low-friction half of the review-date ratchet: a maintainer
    adding a new internal-only Method need only append one trailing comment
    (`# owner=@you review-by=YYYY-MM-DD`) to the line they were already
    adding. Omitting it entirely — the failure mode that let two real
    surfaces (AuditVerify/AuditProveInclusion, the CEP trio) sit unreviewed
    for a long time under reasons ending "bind them in client.py then" — now
    fails here immediately instead of silently parsing as "no metadata".
    """
    entries = _baseline_entries()
    assert entries, "parser found zero baseline entries — check the regex/fixture"
    malformed = [entry.name for entry in entries if not _is_well_formed(entry)]
    assert not malformed, (
        "Baseline entries missing owner + (review-by OR PERMANENT reason): "
        f"{malformed}. Add `# owner=@you review-by=YYYY-MM-DD` (time-boxed "
        "deferral) or `# owner=@you PERMANENT reason=<why>` (genuinely "
        "engine-internal, never gets a Python caller) to each."
    )


def test_baseline_entries_not_past_review_date():
    """The scheduled-decision half of the ratchet: a time-boxed entry whose
    `review-by` date has passed fails the build, forcing someone to either
    bind it, push the date out with a fresh justification, or reclassify it
    PERMANENT — instead of the deferral quietly becoming forever by default.
    """
    entries = _baseline_entries()
    stale = _stale_entries(entries, date.today())
    assert not stale, (
        "Baseline entries past their review-by date (bind, extend the date "
        "with a new reason, or mark PERMANENT with a justification): "
        + ", ".join(
            f"{e.name} (owner={e.owner}, review-by={e.review_by})" for e in stale
        )
    )


def test_review_date_gate_catches_stale_and_honors_fresh():
    """Prove `_stale_entries` fires in both directions against a fixed clock.

    Self-contained (fabricated text, no file I/O, no real wall-clock wait) —
    per the program's own finding that several gates today looked green
    while catching nothing, this asserts the detector actually flags a
    known-bad (overdue) entry AND does NOT flag a known-good (in-window) one,
    AND leaves a PERMANENT entry alone regardless of how old its line is.
    """
    as_of = date(2027, 1, 1)
    text = "\n".join(
        [
            "Overdue  # owner=@proof review-by=2026-12-31 note=one-day-overdue-at-as-of",
            "StillFresh  # owner=@proof review-by=2027-01-02 note=one-day-left-at-as-of",
            "ExactlyDue  # owner=@proof review-by=2027-01-01 note=due-today-not-yet-overdue",
            "NeverExpires  # owner=@proof PERMANENT reason=engine-internal-forever",
        ]
    )
    entries = _parse_entries(text)
    assert {e.name for e in entries} == {
        "Overdue",
        "StillFresh",
        "ExactlyDue",
        "NeverExpires",
    }, "fixture didn't parse as expected — the proof is invalid if this drifts"

    stale_names = {e.name for e in _stale_entries(entries, as_of)}
    assert stale_names == {"Overdue"}, (
        "review-date gate must catch exactly the entry strictly past its "
        f"review-by date at the given as_of; got {stale_names}"
    )


def test_baseline_entries_reject_missing_metadata_fields():
    """A malformed metadata comment (e.g. missing `owner=`, or `PERMANENT`
    with no `reason=`) must be caught by the parser/well-formed check, not
    silently accepted as "no metadata" (which would defeat the ratchet).
    """
    bad_lines = [
        "NoMetadataAtAll",
        "MissingOwner  # review-by=2099-01-01",
        "PermanentNoReason  # owner=@x PERMANENT",
        "BothMissing  # owner=@x",
    ]
    for bad in bad_lines:
        entries = _parse_entries(bad)
        assert len(entries) == 1, f"expected exactly one parsed entry for {bad!r}"
        entry = entries[0]
        assert not _is_well_formed(entry), (
            f"{bad!r} parsed as well-formed but should have failed "
            "test_baseline_entries_well_formed"
        )


@pytest.mark.skip(reason="diagnostic helper, run manually")
def test_print_contract_summary():  # pragma: no cover
    print("variants:", len(_rust_method_variants()))
    print("sent:", len(_python_sent_methods()))
