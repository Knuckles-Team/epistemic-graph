#!/usr/bin/env python3
"""The reviewed register of function pairs dupehound reports that are NOT clones.

Dupehound answers a structural question: after identifiers and literals are
normalized away, does one function have the same shape as another?  That is the
right question for finding copy-paste, and it is why this repository runs it
fail-closed with no baseline.  But shape is not meaning, and a small number of
pairs are structurally identical while being semantically unrelated -- two
`matches!` arms over different literal sets, over different argument types,
answering different questions.  `src/server/http1.rs`'s `is_token_byte(u8)`
(the RFC 7230 `tchar` set) and `src/server/redis_wire/mod.rs`'s
`is_known_command(&str)` (Redis verbs) are the canonical example: merging them
would produce worse code, not better.

Those cannot be "fixed" in the source, and silently lowering the scanner's
sensitivity would hide real clones.  So they are RECORDED here instead, and the
distinction between this and a baseline matters:

* A baseline is machine-written, unexplained, and grows by default.  It converts
  "we have debt" into "we have no debt", which is the ratchet this repository's
  own rules forbid.
* This register is HAND-WRITTEN, carries a stated reason per entry, and is
  ROT-DETECTING: every entry pins the normalized text of BOTH functions.  Change
  either one and its entry stops matching, the finding comes back, and a human
  re-reviews it.  An entry that matches no current finding is reported as stale
  and must be deleted.  Nothing here updates itself.

An entry is therefore a claim with an expiry, not a suppression.

Normalization for the pinned digest collapses runs of whitespace, so `rustfmt`
re-wrapping a signature does not force a re-review, while any token change does.
"""

from __future__ import annotations

import hashlib
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parent.parent
REGISTER_PATH = ROOT / "dupehound-distinct.toml"


class LedgerError(ValueError):
    """The register is absent, malformed, or disagrees with the source tree."""


@dataclass(frozen=True)
class DistinctPair:
    left_file: str
    left_name: str
    left_digest: str
    right_file: str
    right_name: str
    right_digest: str
    reason: str
    reviewed_on: str

    def key(self) -> tuple[str, str, str, str]:
        return (self.left_file, self.left_name, self.right_file, self.right_name)


def normalized_function_text(path: Path, line: int, name: str) -> str | None:
    """Return the brace-balanced source of `name` at (or just after) `line`.

    Dupehound reports a 1-indexed line for the function it matched.  The
    signature may wrap, so the opening brace is found by scanning forward rather
    than assumed to be on that line.  Returns `None` when the named function is
    not there, which the caller treats as a stale register entry rather than a
    silent pass.
    """

    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return None
    lines = source.splitlines()
    if not 1 <= line <= len(lines):
        # A HEAD-relative line can point past the end of the worktree file.
        # That makes the hint useless, not the lookup impossible.
        line = 1
    # Anchor on the DECLARATION, not the reported line. Dupehound reports the
    # `original_*` position against the HEAD blob, so on a branch that has since
    # edited the file those numbers no longer address the worktree -- the line is
    # a hint, never the identity. Prefer a hit near the hint, then fall back to
    # the whole file, so the pin follows the function rather than the offset.
    declaration = re.compile(rf"\bfn\s+{re.escape(name)}\b")
    start = None
    for candidate in range(max(0, line - 3), min(len(lines), line + 3)):
        if declaration.search(lines[candidate]):
            start = candidate
            break
    if start is None:
        for candidate, text in enumerate(lines):
            if declaration.search(text):
                start = candidate
                break
    if start is None:
        return None
    depth = 0
    seen_brace = False
    collected: list[str] = []
    for current in range(start, len(lines)):
        text = lines[current]
        collected.append(text)
        for char in text:
            if char == "{":
                depth += 1
                seen_brace = True
            elif char == "}":
                depth -= 1
        if seen_brace and depth <= 0:
            break
    return re.sub(r"\s+", " ", "\n".join(collected)).strip()


def digest_of(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _string_field(entry: dict[str, Any], field: str, index: int) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise LedgerError(f"register entry {index} has no {field}")
    return value


def load_register(path: Path = REGISTER_PATH) -> list[DistinctPair]:
    if not path.exists():
        return []
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise LedgerError(f"register is unreadable: {error}") from error
    entries = document.get("pair", [])
    if not isinstance(entries, list):
        raise LedgerError("register's `pair` is not an array of tables")
    pairs: list[DistinctPair] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise LedgerError(f"register entry {index} is not a table")
        reason = _string_field(entry, "reason", index)
        if len(reason.split()) < 12:
            raise LedgerError(
                f"register entry {index} has no real justification: an entry is a"
                " reviewed claim, so it must say WHY the two are distinct"
            )
        pairs.append(
            DistinctPair(
                left_file=_string_field(entry, "left_file", index),
                left_name=_string_field(entry, "left_name", index),
                left_digest=_string_field(entry, "left_digest", index),
                right_file=_string_field(entry, "right_file", index),
                right_name=_string_field(entry, "right_name", index),
                right_digest=_string_field(entry, "right_digest", index),
                reason=reason,
                reviewed_on=_string_field(entry, "reviewed_on", index),
            )
        )
    return pairs


def function_exists(path: Path, name: str) -> bool:
    """Is `name` still declared in `path`?"""

    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return re.search(rf"\bfn\s+{re.escape(name)}\b", source) is not None


def delegates_to(path: Path, line: int, name: str, other: str) -> bool:
    """Does `name` merely CALL `other` rather than reimplement it?

    A one-line forwarder -- `foo(a, b)` calling `foo_with_nonce(a, b, None)` --
    has the same normalized shape as its target and is reported as a clone, but
    there is only ONE implementation and nothing to consolidate.  Recognising
    delegation is a statement about the code, not a waiver.
    """

    if name == other:
        # Two distinct functions that happen to share a name (two `Drop` impls,
        # two `as_str`s) cannot delegate to each other, and searching for the
        # name inside the body would match any ordinary call -- `drop(guard)`
        # is not a delegation.
        return False
    body = normalized_function_text(path, line, name)
    if body is None:
        return False
    return re.search(rf"\b{re.escape(other)}\s*\(", body) is not None


def resolved_reason(finding: dict[str, Any], root: Path = ROOT) -> str | None:
    """Why this finding names no duplication that exists, or `None` if it does.

    Dupehound compares the index against HEAD, so on a branch that renames,
    splits or deletes a file, it faithfully reports the OLD copy -- which is
    already gone -- as the thing being reimplemented.  That is not debt anyone
    can pay down: there is no second implementation left to delete.  The same
    holds when one of the pair simply forwards to the other.
    """

    file = root / str(finding["file"])
    original_file = root / str(finding["original_file"])
    name = str(finding["name"])
    original_name = str(finding["original_name"])
    if not function_exists(original_file, original_name):
        return (
            f"{finding['original_file']}::{original_name} is no longer in the tree"
            " -- the other implementation was already removed"
        )
    if not function_exists(file, name):
        return f"{finding['file']}::{name} is no longer in the tree"
    if delegates_to(file, int(finding["line"]), name, original_name):
        return f"{name} delegates to {original_name} rather than reimplementing it"
    if delegates_to(original_file, int(finding["original_line"]), original_name, name):
        return f"{original_name} delegates to {name} rather than reimplementing it"
    return None


def partition(
    findings: Iterable[dict[str, Any]],
    pairs: list[DistinctPair],
    root: Path = ROOT,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[DistinctPair]]:
    """Split findings into (unregistered, changed, notes, unused-register-entries).

    `unregistered` are real gate failures.  `changed` are findings whose pair IS
    registered but whose source no longer matches the reviewed text -- the
    register has rotted and those must fail too, loudly and by a different name,
    because the review they carry no longer describes the code.

    Rot is checked against the SOURCE, not against whether a finding happens to
    be reported.  Dupehound only reports a pair when one of its files is in the
    changed-source scope, so a perfectly valid entry is silent on most runs;
    failing on "matched nothing" would make the register unusable.  What must
    never go unnoticed is an entry whose functions have since changed or
    disappeared, and that is decidable from the tree alone.
    """

    by_key = {pair.key(): pair for pair in pairs}
    unregistered: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    notes: list[str] = []
    matched: set[tuple[str, str, str, str]] = set()
    for finding in findings:
        # Findings that name no live second implementation are not debt; see
        # `resolved_reason`. They are reported, not counted against the gate.
        resolved = resolved_reason(finding, root)
        if resolved is not None:
            notes.append(
                f"resolved: {finding['file']}:{finding['line']} {finding['name']}"
                f" -- {resolved}"
            )
            continue
        key = (
            str(finding["file"]),
            str(finding["name"]),
            str(finding["original_file"]),
            str(finding["original_name"]),
        )
        reverse = (key[2], key[3], key[0], key[1])
        pair = by_key.get(key) or by_key.get(reverse)
        if pair is None:
            unregistered.append(finding)
            continue
        # Resolve exactly as the rot check below does -- first declaration of
        # that name in the file. A file may declare the same name more than once
        # (`as_str` on several enums), so using the finding's line hint here and
        # a plain search there would pin two different functions and report a
        # change that never happened.
        left = normalized_function_text(root / pair.left_file, 1, pair.left_name)
        right = normalized_function_text(root / pair.right_file, 1, pair.right_name)
        if left is None or right is None:
            changed.append(finding)
            notes.append(
                f"{pair.left_file}::{pair.left_name} / {pair.right_file}::"
                f"{pair.right_name}: a registered function could not be located"
            )
            continue
        if digest_of(left) != pair.left_digest or digest_of(right) != pair.right_digest:
            changed.append(finding)
            notes.append(
                f"{pair.left_file}::{pair.left_name} / {pair.right_file}::"
                f"{pair.right_name}: reviewed {pair.reviewed_on}, but the source"
                " has changed since -- re-review and re-pin, or consolidate"
            )
            continue
        matched.add(pair.key())
    # Independently of what was reported this run, every entry must still
    # describe the code it was reviewed against.
    rotted: list[DistinctPair] = []
    for pair in pairs:
        left = normalized_function_text(root / pair.left_file, 1, pair.left_name)
        right = normalized_function_text(root / pair.right_file, 1, pair.right_name)
        if left is None or right is None:
            rotted.append(pair)
            notes.append(
                f"{pair.left_file}::{pair.left_name} / {pair.right_file}::"
                f"{pair.right_name}: a reviewed function no longer exists --"
                " delete this entry"
            )
            continue
        if digest_of(left) != pair.left_digest or digest_of(right) != pair.right_digest:
            rotted.append(pair)
            notes.append(
                f"{pair.left_file}::{pair.left_name} / {pair.right_file}::"
                f"{pair.right_name}: reviewed {pair.reviewed_on}, but the source has"
                " changed since -- re-review and re-pin, or consolidate"
            )
    return unregistered, changed, notes, rotted


def main() -> int:
    """Print the digests for a pair, to author or re-pin a register entry."""

    if len(sys.argv) != 5:
        print(
            "usage: dupehound_ledger.py <left_file> <left_name> <right_file> "
            "<right_name>",
            file=sys.stderr,
        )
        return 2
    left_file, left_name, right_file, right_name = sys.argv[1:5]
    for relative, name in ((left_file, left_name), (right_file, right_name)):
        path = ROOT / relative
        source = path.read_text(encoding="utf-8") if path.exists() else ""
        found = None
        for index, text in enumerate(source.splitlines(), start=1):
            if re.search(rf"\bfn\s+{re.escape(name)}\b", text):
                found = normalized_function_text(path, index, name)
                break
        if found is None:
            print(f"{relative}::{name}: NOT FOUND", file=sys.stderr)
            return 1
        print(f"{relative}::{name} = {digest_of(found)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
