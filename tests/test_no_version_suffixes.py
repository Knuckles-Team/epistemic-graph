#!/usr/bin/env python3
"""RF-ADR-006 source guard: component names carry no version suffix.

Fails if any ``\\b[A-Z][A-Za-z0-9]*V[0-9]+\\b`` identifier or ``\\b[a-z_]+_v[0-9]+\\b``
table-shaped token appears in the scanned roots outside:

  * the explicit ``ALLOWLIST`` below, which RF-ADR-006 requires stay EMPTY --
    every legitimate exception is expressed as a pattern rule with a written
    reason in ``FALSE_POSITIVE_LINE_PATTERNS``, never as a bare name here, and
  * this file itself, whose planted-bad fixtures are test data (see
    ``GUARD_SELF_PATH``).

Numeric format-identity constants (``*_SCHEMA_VERSION``, ``*_FORMAT_VERSION``,
``*_INCARNATION``) are component *identities*, not component names, and are
exempt by construction: SCREAMING_SNAKE_CASE matches neither regex.

stdlib-only (re, pathlib, unittest). Runs as a pre-commit hook and standalone:
``python3 tests/test_no_version_suffixes.py``.
"""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# RF-ADR-006: "a source guard that fails on any new ... identifier or table
# name outside an explicitly EMPTY allowlist". This MUST stay empty -- any real
# exception is a pattern rule below (with its reason), not a name added here.
ALLOWLIST: frozenset[str] = frozenset()

ID_RE = re.compile(r"\b[A-Z][A-Za-z0-9]*V[0-9]+\b")
TBL_RE = re.compile(r"\b[a-z_]+_v[0-9]+\b")

ROOTS = ("crates", "src", "tests", "epistemic_graph", "docs", "scripts", "contract")
EXTRA_FILES = (".pre-commit-config.yaml",)
TEXT_SUFFIXES = {".rs", ".py", ".yaml", ".yml", ".md", ".toml"}

# This guard's own planted-bad fixtures and its pattern literals are test data,
# not source. Excluding exactly one file (itself) keeps the scan honest without
# an allowlist; `test_self_exclusion_is_exactly_one_file` pins the exclusion.
GUARD_SELF_NAME = "test_no_version_suffixes.py"

# Pattern-based false-positive rules. A matched token whose whole LINE also
# matches one of these regexes is excused. Each rule carries the reason it is
# not a component name; grouped by reason, never by "this one name is fine".
FALSE_POSITIVE_LINE_PATTERNS: list[tuple[re.Pattern, str]] = [
    # -- External crate / protocol / vendor proper names ending in V<n>. -----
    (re.compile(r"openraft|RaftNetworkV\d"), "external openraft crate trait"),
    (re.compile(r"SigV4"), "AWS SigV4 request-signing protocol name"),
    (
        re.compile(
            r"\bSV1\b.*(?:DM1|TN1|Braket|simulator)|(?:DM1|TN1|Braket|simulator).*\bSV1\b"
        ),
        "AWS Braket simulator SKU",
    ),
    (
        re.compile(r"Mipro ?V2|mipro_v2|MIPROv2|specs_mipro_v2"),
        "external DSPy MIPROv2 algorithm name",
    ),
    (re.compile(r"ListObjectsV2"), "AWS S3 REST API operation name"),
    (re.compile(r"Uuid::new_v4|::new_v4\("), "uuid crate's new_v4() constructor"),
    (
        re.compile(r"mvhd_box_v0|mvhd"),
        "ISO-BMFF mvhd box version field (external media spec)",
    ),
    # -- IP protocol version (v4/v6), not a schema/component version. --------
    (
        re.compile(r"is_ssrf_(?:custom|std)_range_v[46]"),
        "IPv4/IPv6 SSRF range check, not a version suffix",
    ),
    # -- Retired-name quarantine markers: renaming them would break the very --
    # -- guards that assert the retired spelling never reappears. ------------
    (
        re.compile(
            r"RETIRED_PROTOTYPE_TABLES|CompatibilityMsgpackV1|compatibility_msgpack_v1|"
            r"method_policy_hierarchy_v1|\bmutation_[a-z_]+_v3\b|not in "
            r"(?:scanner_source|wire|stream)"
        ),
        "retired-name quarantine / must-not-reappear marker; the historical spelling "
        "is the guard",
    ),
    (
        re.compile(r'^\s*"[a-z_]+_v[0-9]+",?\s*(?://.*)?$'),
        "bare string-literal entry in a retired-name quarantine list",
    ),
    # -- Durable EVENT-TYPE tag strings: names of recorded events, not of ----
    # -- components, tables, types or functions. -----------------------------
    (
        re.compile(r"\b(?:blob_[a-z_]+|served_modality|sparql_http_[a-z_]+)_v[0-9]+\b"),
        "durable event-type tag string (maintenance/outbox event name), not a table or "
        "component name",
    ),
    # -- Transient SQL codegen aliases; never persisted. ---------------------
    (
        re.compile(r"_pgq_v\d"),
        "transient SQL property-graph-query codegen alias, not persisted storage",
    ),
    # -- Local bindings, fixture labels and doc example values. --------------
    (
        re.compile(
            r"\b(?:primary|secondary|content|model|want|r|page_token|fixed_params)_v[0-9]+\b"
        ),
        "local binding / test-fixture label / doc example value, not a component name",
    ),
    (
        re.compile(r"tests/fixtures/[a-z_]+_v[0-9]+\.json|method_policy_registry_v1"),
        "test fixture FILENAME, not a persisted table",
    ),
]


def iter_target_files(root: Path):
    for sub in ROOTS:
        subdir = root / sub
        if not subdir.is_dir():
            continue
        for path in sorted(subdir.rglob("*")):
            if (
                path.is_file()
                and path.suffix in TEXT_SUFFIXES
                and path.name != GUARD_SELF_NAME
            ):
                yield path
    for extra in EXTRA_FILES:
        path = root / extra
        if path.is_file():
            yield path


def _is_excused(line: str) -> str | None:
    for pattern, reason in FALSE_POSITIVE_LINE_PATTERNS:
        if pattern.search(line):
            return reason
    return None


def _line_violations(
    path: Path, lineno: int, line: str
) -> list[tuple[Path, int, str, str]]:
    """Every (file, line_number, token, line_text) violation on one line, or
    ``[]`` if the line is excused or carries no version-suffixed token."""
    if _is_excused(line) is not None:
        return []
    return [
        (path, lineno, match.group(0), line.strip()[:160])
        for regex in (ID_RE, TBL_RE)
        for match in regex.finditer(line)
        if match.group(0) not in ALLOWLIST
    ]


def find_violations(root: Path) -> list[tuple[Path, int, str, str]]:
    """Returns a list of (file, line_number, token, line_text) violations."""
    violations: list[tuple[Path, int, str, str]] = []
    for path in iter_target_files(root):
        try:
            text = path.read_text(encoding="utf-8", errors="strict")
        except (UnicodeDecodeError, OSError):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            violations.extend(_line_violations(path, lineno, line))
    return violations


class NoVersionSuffixesTest(unittest.TestCase):
    """RF-ADR-006: component, crate, type, trait, function, module, slice and
    table names carry no V<n>/_v<n> suffix."""

    # This file lives at tests/test_no_version_suffixes.py, so parents[1] is the
    # repo root. RF_ADR_006_GUARD_ROOT points it at an arbitrary worktree.
    ROOT = (
        Path(os.environ["RF_ADR_006_GUARD_ROOT"]).resolve()
        if os.environ.get("RF_ADR_006_GUARD_ROOT")
        else Path(__file__).resolve().parents[1]
    )

    def test_no_stray_version_suffixed_names(self):
        violations = find_violations(self.ROOT)
        if violations:
            lines = [
                f"{p.relative_to(self.ROOT)}:{lineno}: {token!r}  ({text})"
                for p, lineno, token, text in violations[:50]
            ]
            more = (
                ""
                if len(violations) <= 50
                else f"\n... and {len(violations) - 50} more"
            )
            self.fail(
                f"{len(violations)} version-suffixed name(s) found outside the "
                f"allowlist and the "
                f"false-positive patterns (RF-ADR-006):\n" + "\n".join(lines) + more
            )

    def test_allowlist_is_empty(self):
        """RF-ADR-006 requires the allowlist to be explicitly empty; any real
        exception must be a pattern rule with a reason, not a bare name."""
        self.assertEqual(
            ALLOWLIST, frozenset(), "ALLOWLIST must stay empty per RF-ADR-006"
        )

    def test_self_exclusion_is_exactly_one_file(self):
        """The only file the scan skips is this guard itself."""
        scanned = {p.name for p in iter_target_files(self.ROOT)}
        self.assertNotIn(GUARD_SELF_NAME, scanned)
        self.assertTrue(
            (self.ROOT / "tests" / GUARD_SELF_NAME).is_file(),
            "the guard excludes a file name that does not exist -- the exclusion is "
            "stale",
        )

    def test_guard_actually_catches_a_known_bad_identifier(self):
        """Planted-bad check: prove the guard is not blind. A synthetic
        version-suffixed identifier and table name must be flagged, and must
        NOT be excused by any false-positive pattern."""
        planted_id = "TotallyFakeComponent" + "V7"
        planted_table = "totally_fake_table" + "_v7"
        sample = (
            f"struct {planted_id} {{}}\n"
            f'const T: TableDefinition = TableDefinition::new("{planted_table}");\n'
        )
        self.assertIsNone(
            _is_excused(sample),
            "planted-bad sample must not match any false-positive pattern",
        )
        self.assertIn(
            planted_id,
            ID_RE.findall(sample),
            "guard regex missed a planted-bad PascalCase identifier",
        )
        self.assertIn(
            planted_table,
            TBL_RE.findall(sample),
            "guard regex missed a planted-bad table name",
        )

    def test_guard_catches_a_planted_bad_file_end_to_end(self):
        """Planted-bad check through the real file walk, not just the regexes:
        a temporary source file carrying a suffixed name must be reported by
        find_violations() with its path and line."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "crates").mkdir()
            (root / "tests").mkdir()
            (root / "tests" / GUARD_SELF_NAME).write_text(
                "# stand-in for the guard itself\n"
            )
            bad = root / "crates" / "planted.rs"
            bad.write_text("pub struct " + "PlantedKernel" + "V3 {}\n")
            found = find_violations(root)
            self.assertEqual(
                len(found), 1, f"expected exactly one violation, got {found}"
            )
            self.assertEqual(found[0][0], bad)
            self.assertEqual(found[0][2], "PlantedKernel" + "V3")

    def test_guard_catches_function_name_suffix(self):
        """RF-ADR-006 covers function names too. A snake_case ``fn ..._v<n>(``
        matches the table-shaped regex and must be caught exactly like a table
        -- a distinct planted-bad context from a TableDefinition::new(...)."""
        planted_fn = "totally_fake_method" + "_v9"
        sample = (
            f"fn {planted_fn}(req: &Request) -> Result<(), String> {{\n    Ok(())\n}}\n"
        )
        self.assertIsNone(
            _is_excused(sample),
            "planted-bad function sample must not match any excuse pattern",
        )
        self.assertIn(
            planted_fn,
            TBL_RE.findall(sample),
            "guard regex missed a planted-bad function name",
        )

    def test_known_false_positives_are_excused(self):
        """Sanity-check the excuse patterns actually fire, so a future edit
        cannot silently stop excusing something real and red the whole suite."""
        for line in (
            "impl RaftNetworkV2 for TcpNetwork {}",
            "hasher.update(uuid::Uuid::new_v4().as_bytes());",
            '    "mutation_store_root_v3",',
            '        self.maintain("blob_sweep_v1", batch)?;',
            '        let want_v2 = hierarchy == "0";',
        ):
            self.assertIsNotNone(
                _is_excused(line), f"expected an excuse pattern to fire for: {line!r}"
            )

    def test_every_false_positive_pattern_carries_a_reason(self):
        for pattern, reason in FALSE_POSITIVE_LINE_PATTERNS:
            self.assertTrue(
                reason and len(reason) > 20,
                f"pattern {pattern.pattern!r} has no written reason",
            )


if __name__ == "__main__":
    unittest.main()
