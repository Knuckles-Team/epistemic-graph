"""EH-308: the jscpd differential gate's identity must survive a fragment
relocating (rename, split, directory move, ...) between the base tree and
HEAD, while still catching a genuinely new occurrence of duplicated content.

Ruling basis: DECISIONS.md 2026-09-20 ("fix the gate's identity function, do
not re-baseline"; the fix must be proved against a known-bad input). These
tests are exactly that proof, at the unit level the gate never had before
this change: a synthetic "before" and "after" jscpd report, compared the
same way `enforce()` compares two real snapshots via `keys()`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _script_loader import load_script

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _clone(root: Path, first: str, second: str, fragment: str = "return value") -> dict:
    return {
        "format": "python",
        "fragment": fragment,
        "lines": 2,
        "tokens": 4,
        "firstFile": {
            "name": str(root / first),
            "startLoc": {"line": 2},
            "endLoc": {"line": 3},
        },
        "secondFile": {
            "name": str(root / second),
            "startLoc": {"line": 4},
            "endLoc": {"line": 5},
        },
    }


def _report(*clones: dict) -> dict:
    return {"duplicates": list(clones)}


def test_relocated_fragment_between_new_file_pair_is_not_new(tmp_path):
    """(a) A fragment that only MOVED -- same content, different file pair,
    same total occurrence count -- must not read as a new pair."""
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"

    before = _report(_clone(root, "src/monolith.py", "src/other.py"))
    after = _report(_clone(root, "src/monolith/part_a.py", "src/other.py"))

    before_keys = jscpd.keys(before, root)
    after_keys = jscpd.keys(after, root)

    assert after_keys - before_keys == set()


def test_second_occurrence_of_known_content_is_still_new(tmp_path):
    """(b) The SAME fragment content appearing a second time -- even though
    its digest already existed in the base tree elsewhere -- is still
    reported as new: the ordinal only suppresses a pair when the total
    occurrence count did not increase."""
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"

    before = _report(_clone(root, "src/a.py", "src/b.py"))
    after = _report(
        _clone(root, "src/a.py", "src/b.py"),
        _clone(root, "src/c.py", "src/d.py"),  # a genuinely new occurrence
    )

    before_keys = jscpd.keys(before, root)
    after_keyed = jscpd.keyed_originals(after, root)
    new_pairs = set(after_keyed) - before_keys

    assert len(new_pairs) == 1
    (new_key,) = new_pairs
    assert after_keyed[new_key] in (("src/c.py", "src/d.py"), ("src/d.py", "src/c.py"))


def test_a_wholly_new_fragment_is_caught(tmp_path):
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"

    before = _report(_clone(root, "src/a.py", "src/b.py", fragment="return value"))
    after = _report(
        _clone(root, "src/a.py", "src/b.py", fragment="return value"),
        _clone(root, "src/x.py", "src/y.py", fragment="def totally_new(): pass"),
    )

    new_pairs = jscpd.keys(after, root) - jscpd.keys(before, root)
    assert len(new_pairs) == 1


def test_ordinal_assignment_is_deterministic_across_runs(tmp_path):
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"
    document = _report(
        _clone(root, "src/a.py", "src/b.py"),
        _clone(root, "src/c.py", "src/d.py"),
        _clone(root, "src/e.py", "src/f.py"),
    )

    first_run = jscpd.keys(document, root)
    second_run = jscpd.keys(document, root)

    assert first_run == second_run
    assert len(first_run) == 3
    ordinals = sorted(key[2] for key in first_run)
    assert ordinals == [0, 1, 2]


def test_whitespace_and_known_receiver_prefix_still_normalise_to_one_identity(tmp_path):
    """Pre-existing normalisation (indentation + the named ctx./context./
    coordination. receiver-prefix rewrite) must still collapse to the same
    digest after the path-independent identity change."""
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"

    indented = _clone(
        root, "src/a.py", "src/b.py", fragment="    ctx.req.id\n    return ctx.req.id\n"
    )
    reindented = _clone(
        root, "src/c.py", "src/d.py", fragment="ctx.req.id\nreturn ctx.req.id"
    )

    (key_indented,) = jscpd.keys(_report(indented), root)
    (key_reindented,) = jscpd.keys(_report(reindented), root)

    assert key_indented[:2] == key_reindented[:2]  # same (format, digest)


def test_genuinely_different_fragments_never_collide(tmp_path):
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"

    one = _clone(root, "src/a.py", "src/b.py", fragment="return 1")
    other = _clone(root, "src/c.py", "src/d.py", fragment="return 2")

    keys = jscpd.keys(_report(one, other), root)
    assert len(keys) == 2


def test_rust_import_only_fragments_do_not_count_as_behavior_clones(tmp_path):
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"
    imports = _clone(
        root,
        "src/server/federation/mod.rs",
        "src/server/kvcache_http/mod.rs",
        fragment=(
            "use std::time::Duration;\n\n"
            "use tokio::io::AsyncWriteExt;\n"
            "use tokio::net::TcpListener;\n"
            "use tokio::sync::RwLock;\n\n"
            "use crate::server::http1::{self, HttpMessage, RequestLimits};\n"
            "use crate::server::ServerState;\n"
            "use eg_plan::federation::{pinned_http_agent, PinnedHttpTimeouts};"
        ),
    )
    imports["format"] = "rust"

    assert jscpd.keys(_report(imports), root) == set()


def test_import_rule_keeps_executable_rust_and_other_formats(tmp_path):
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"
    executable = _clone(
        root,
        "src/a.rs",
        "src/b.rs",
        fragment="use crate::State;\nfn request() { dispatch(); }",
    )
    executable["format"] = "rust"
    partial = _clone(
        root,
        "src/c.rs",
        "src/d.rs",
        fragment="use crate::State;\nlet state = State::new();",
    )
    partial["format"] = "rust"
    python_import = _clone(
        root,
        "src/e.py",
        "src/f.py",
        fragment="use crate::State;",
    )

    assert len(jscpd.keys(_report(executable, partial, python_import), root)) == 3


def test_reviewed_trait_boundary_pin_requires_exact_fragment_paths_and_lines(tmp_path):
    jscpd = load_script("check_duplication")
    root = tmp_path / "repo"
    fragment = (
        "            iceberg_predicate_for(filter, &self.schema).is_some()\n"
        "        })\n"
        "    }\n\n"
        "    async fn scan(\n"
        "        &self,\n"
        "        state: &dyn Session,\n"
        "        projection: Option<&Vec<usize>>,\n"
        "        filters: &[Expr],\n"
        "        limit: Option<usize>,\n"
        "    ) -> DfResult<Arc<dyn ExecutionPlan>> {\n"
        "        let request = IcebergScanRequest::new("
        "&self.schema, projection, filters, limit)?;"
    )
    clone = _clone(
        root,
        "crates/eg-query/src/sql/iceberg_federation.rs",
        "crates/eg-query/src/sql/providers.rs",
        fragment=fragment,
    )
    clone["format"] = "rust"
    clone["firstFile"]["startLoc"]["line"] = 474
    clone["secondFile"]["startLoc"]["line"] = 484

    assert jscpd.keys(_report(clone), root) == set()
    clone["secondFile"]["startLoc"]["line"] = 485
    assert len(jscpd.keys(_report(clone), root)) == 1
    clone["secondFile"]["startLoc"]["line"] = 484
    clone["fragment"] += "\n    execute_more();"
    assert len(jscpd.keys(_report(clone), root)) == 1
    clone["fragment"] = fragment
    clone["secondFile"]["name"] = str(root / "src/other.rs")
    assert len(jscpd.keys(_report(clone), root)) == 1
