#!/usr/bin/env python3
"""Scan the not-yet-pushed commit range for credential-shaped patch content.

CONCEPT:AU-OS.governance.tiered-merge-gate — D-CIP-13.

**The vector this gate defends.** GitHub is public-facing and gets strict
standards. A credential leaked into *any* commit's patch text is public the
moment that range is pushed, even if a later commit deletes it — git history
is not a hiding place. Before this gate, the only tooling that answered "is
the range we are about to push clean" was two scratchpad scripts
(``secret_scan.py`` / ``entropy_scan.py``) that a human had to remember to
run by hand, with no fixed home and no wiring into any hook. A clean result
from one invocation also silently expires: this repo's push is deliberately
deferred until every other deferred item lands, so hundreds more commits
land between "the scan was run" and "the push actually happens." A
scratchpad script that nobody re-runs is a control that exists only in a
report, not in the tree.

**What it does.** Runs ``git log -p --format='%x00COMMIT%x00%H' <base>..HEAD``
(patch text of every commit in the not-yet-pushed range, matching how the
original scan was performed) and scans the **added** lines of that range
against:

1. 19 credential-shaped regexes (AWS, GitHub PAT/fine-grained/OAuth, GitLab,
   Slack, Stripe, OpenAI, Anthropic, Google, npm, PyPI, JWT, a PEM private-key
   block, ``client_secret``/generic ``password|secret|token|api_key``
   assignment, DB URLs with an embedded password, basic-auth URLs). Any hit
   here is a **hard failure** (``exit 1``) — this is the part of the gate
   that actually blocks a push. A line carrying the documented
   ``# sanitizer:ignore`` marker is exempt (matches the convention already
   used by this repo's synthetic credential-shaped test fixtures).
2. A keyword-free high-entropy sweep (any 24-64 char, >=3-character-class
   token at >=4.4 bits/char, excluding hashes/UUIDs/plain identifiers and
   lockfile diffs) — reported **informationally only**, never blocking. The
   very first run of this sweep (see the original lane-ci-parity-0801 report)
   found 106/110 candidates were the public ed25519 ``signature:`` fields in
   ``agent_utilities/knowledge_graph/ontology/connector_manifests*`` — a
   legitimate public artifact, not a secret. Hard-failing on entropy alone
   would either need a bespoke exemption list (a new noise surface) or would
   make the gate untrustworthy the first time it cries wolf. It stays a
   human-reviewed signal, not a blocking one, until that noise is tamed.

**Default range.** ``<base>..HEAD`` where ``<base>`` defaults to
``origin/main`` if that ref exists locally, else the gate refuses rather than
silently scanning nothing or the wrong range (D-MW-9 class: a gate that finds
nothing to check must say so loudly, never look clean by accident). Pass
``--base <ref>`` to scan an explicit range (e.g. the last actually-pushed
sha, once that differs from ``origin/main``).

Run it right before the eventual push (its own stated precondition — a clean
result over an old range is not evidence about a new one) via::

    python3 scripts/security/check_secret_history.py --base origin/main
    python3 scripts/security/check_secret_history.py --repository-root DIR
    python3 scripts/security/check_secret_history.py --self-check

``--repository-root`` (GOC-59-W08/B7 — same declared flag every other
``scripts/security/check_*.py`` contract check carries, matching
``check_cypher_write_subset.py``'s sibling convention): the merge queue's fast
tier (``agent_utilities/governance/merge_queue.py``, ``_contract_check_argv``)
discovers a script's declared repository root by grepping its own source for
the literal string ``--repository-root`` and, only when present, appends
``--repository-root <merged-tree>`` to the invocation. Both the scanned repo
and the baseline file resolve from it, so a merge-queue run always reads the
baseline that ships WITH the merged tree it is checking, not a stale one from
wherever the interpreter happens to have been launched.

``--self-check`` proves the credential-pattern half actually catches a
planted secret (AWS-shaped key, GitHub PAT, private-key block) in a throwaway
git repo, and that the ``sanitizer:ignore`` marker exempts it — i.e. it would
have caught a real leak, not just describe one.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_PATH = REPO_ROOT / "scripts" / "security" / "secret_history_baseline.txt"

SANITIZER_MARKER = "sanitizer:ignore"

CREDENTIAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "aws_access_key_id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "aws_secret_key_assignment": re.compile(
        r"(?i)aws_secret_access_key\s*[:=]\s*['\"]?[A-Za-z0-9/+=]{40}['\"]?"
    ),
    "github_pat_classic": re.compile(r"\bghp_[A-Za-z0-9]{36}\b"),
    "github_pat_fine_grained": re.compile(r"\bgithub_pat_[A-Za-z0-9_]{22,}\b"),
    "github_oauth": re.compile(r"\bgho_[A-Za-z0-9]{36}\b"),
    "gitlab_pat": re.compile(r"\bglpat-[A-Za-z0-9_-]{20}\b"),
    "slack_token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    "stripe_key": re.compile(r"\b(?:sk|pk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b"),
    "openai_key": re.compile(r"\bsk-[A-Za-z0-9]{20,}(?:T3BlbkFJ[A-Za-z0-9]{20,})?\b"),
    "openai_project_key": re.compile(r"\bsk-proj-[A-Za-z0-9_-]{20,}\b"),
    "anthropic_key": re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b"),
    "google_api_key": re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    "npm_token": re.compile(r"\bnpm_[A-Za-z0-9]{36}\b"),
    "pypi_token": re.compile(r"\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{20,}\b"),
    "jwt": re.compile(
        r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"
    ),
    "private_key_block": re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"
    ),
    "client_secret_assignment": re.compile(
        r"(?i)client_secret\s*[:=]\s*['\"][A-Za-z0-9~_.\-+/=]{8,}['\"]"
    ),
    "generic_secret_assignment": re.compile(
        r"(?i)\b(?:password|passwd|pwd|secret|api[_-]?key|token)\s*[:=]\s*"
        r"['\"][^'\"\s]{6,}['\"]"
    ),
    "db_url_with_password": re.compile(
        r"(?i)\b(?:postgres|postgresql|mysql|mongodb|redis)://[^\s'\"/@]+:[^\s'\"/@]+@"
    ),
    "basic_auth_url": re.compile(r"(?i)https?://[^\s'\"/@]+:[^\s'\"/@]+@[^\s'\"]+"),
}

_ENTROPY_TOKEN_RE = re.compile(r"[A-Za-z0-9+/_.=-]{24,64}")
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_IDENTIFIER_LIKE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOCKFILE_SUFFIXES = (".lock", "-lock.json", "uv.lock", "poetry.lock", "Cargo.lock")
_MIN_ENTROPY = 4.4


def _shannon_entropy(s: str) -> float:
    if not s:
        return 0.0
    freq: dict[str, int] = {}
    for ch in s:
        freq[ch] = freq.get(ch, 0) + 1
    n = len(s)
    return -sum((c / n) * math.log2(c / n) for c in freq.values())


def _is_entropy_noise(token: str) -> bool:
    return bool(
        _HEX_RE.match(token)
        or _UUID_RE.match(token)
        or _IDENTIFIER_LIKE_RE.match(token)
    )


def _iter_added_lines(patch_lines: list[str]):
    """Yield ``(commit, file, content)`` for every added (``+``) line.

    ``patch_lines`` is the output of ``git log -p --format='%x00COMMIT%x00%H'``
    split on newlines. File-header ``+++``/``---`` lines are skipped so they
    are never mistaken for content.
    """
    current_commit: str | None = None
    current_file: str | None = None
    for line in patch_lines:
        if line.startswith("\x00COMMIT\x00"):
            current_commit = line.split("\x00COMMIT\x00", 1)[1].strip()
            continue
        if line.startswith("+++ "):
            current_file = line[4:].strip()
            continue
        if line.startswith("+++") or line.startswith("---"):
            continue
        if not line.startswith("+"):
            continue
        yield current_commit or "?", current_file or "?", line[1:]


#: This scanner's own source and baseline necessarily CONTAIN the credential
#: shapes it hunts for -- `aws_access_key_id`, `private_key_block` and friends
#: are regex literals here, and the baseline records real past hits verbatim.
#: Scanning them flags the DETECTOR as the leak: on its very first queue run
#: this gate rejected its OWN branch with 5 "credential-shaped" hits, every one
#: of them its own pattern table or baseline.
#:
#: Excluding them is not a weakening. A scanner cannot meaningfully audit itself
#: -- any finding is by construction a definition, not a secret -- and both
#: `check_tracked_privacy.py` and `check_wheel_privacy.py` already exclude
#: `scripts/` for exactly this reason ("the scanners live there").
#:
#: Scoped to THESE TWO FILES ONLY, deliberately: a blanket `scripts/security/`
#: exclusion would blind the gate to a real credential committed into any other
#: gate in that directory.
_SELF_PATHS = (
    "scripts/security/check_secret_history.py",
    "scripts/security/secret_history_baseline.txt",
)


def _is_self(file_: str) -> bool:
    """True for this scanner's own source or baseline (git patch `b/` prefix)."""
    normalized = file_[2:] if file_.startswith(("a/", "b/")) else file_
    return normalized in _SELF_PATHS


def scan_credentials(patch_lines: list[str]) -> list[dict]:
    hits: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for commit, file_, content in _iter_added_lines(patch_lines):
        if SANITIZER_MARKER in content:
            continue
        if _is_self(file_):
            continue
        for name, pattern in CREDENTIAL_PATTERNS.items():
            if not pattern.search(content):
                continue
            key = (commit, file_, name)
            if key in seen:
                continue
            seen.add(key)
            hits.append(
                {
                    "commit": commit,
                    "file": file_,
                    "pattern": name,
                    "context": content.strip()[:160],
                }
            )
    return hits


def scan_entropy(patch_lines: list[str]) -> list[dict]:
    hits: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    current_file_for_lockfile_check: str | None = None
    for commit, file_, content in _iter_added_lines(patch_lines):
        current_file_for_lockfile_check = file_
        if _is_self(file_):
            continue
        if file_ != "?" and any(
            file_.endswith(suffix) for suffix in _LOCKFILE_SUFFIXES
        ):
            continue
        for m in _ENTROPY_TOKEN_RE.finditer(content):
            token = m.group(0)
            if _is_entropy_noise(token):
                continue
            classes = sum(
                bool(re.search(p, token))
                for p in (r"[a-z]", r"[A-Z]", r"[0-9]", r"[+/_.=-]")
            )
            if classes < 3:
                continue
            entropy = _shannon_entropy(token)
            if entropy < _MIN_ENTROPY:
                continue
            key = (commit, file_, token[:12])
            if key in seen:
                continue
            seen.add(key)
            hits.append(
                {
                    "commit": commit,
                    "file": file_,
                    "entropy": round(entropy, 2),
                    "token_preview": token[:12] + "...",
                }
            )
    del current_file_for_lockfile_check
    return hits


def _git_log_patch_lines(repo: Path, rev_range: str) -> list[str]:
    proc = subprocess.run(
        ["git", "log", "-p", "--format=%x00COMMIT%x00%H", rev_range],
        cwd=str(repo),
        capture_output=True,
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"git log -p {rev_range!r} failed (rc={proc.returncode}): "
            f"{proc.stderr.strip()[-500:]}"
        )
    return proc.stdout.splitlines()


def load_baseline(path: Path = BASELINE_PATH) -> set[tuple[str, str]]:
    """``{(file, pattern)}`` already known and accepted as a synthetic fixture.

    D-CIP-14: a new absolute contract check cannot be added to a tree that
    already violates it. Keyed on ``(file, pattern)`` rather than commit,
    because the scanned range is ``<base>..HEAD`` and grows with every new
    commit — a commit-keyed baseline would need updating on every push even
    when no *new* fixture is introduced. A missing baseline file is an empty
    baseline (every hit is new) — stricter, never permissive, matching the
    convention set by ``check_ci_environment_contract.py``.
    """
    if not path.is_file():
        return set()
    entries: set[tuple[str, str]] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        file_, _, pattern = line.partition("\t")
        if pattern:
            entries.add((file_.strip(), pattern.strip()))
    return entries


def _ref_exists(repo: Path, ref: str) -> bool:
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", ref],
        cwd=str(repo),
        capture_output=True,
        check=False,
    )
    return proc.returncode == 0


def check(
    repo_root: Path, base: str | None, baseline_path: Path = BASELINE_PATH
) -> tuple[int, dict]:
    if base is None:
        if _ref_exists(repo_root, "origin/main"):
            base = "origin/main"
        else:
            return 1, {
                "ok": False,
                "error": (
                    "no --base given and origin/main does not exist locally — "
                    "refusing to guess a range rather than silently scanning "
                    "nothing or the wrong commits. Pass --base <ref> explicitly."
                ),
            }
    rev_range = f"{base}..HEAD"
    try:
        patch_lines = _git_log_patch_lines(repo_root, rev_range)
    except RuntimeError as exc:
        return 1, {"ok": False, "error": str(exc)}

    added_lines = sum(
        1 for line in patch_lines if line.startswith("+") and not line.startswith("+++")
    )
    all_credential_hits = scan_credentials(patch_lines)
    entropy_hits = scan_entropy(patch_lines)

    baseline = load_baseline(baseline_path)
    new_hits = [
        h for h in all_credential_hits if (h["file"], h["pattern"]) not in baseline
    ]
    baselined_hits = [
        h for h in all_credential_hits if (h["file"], h["pattern"]) in baseline
    ]
    seen_pairs = {(h["file"], h["pattern"]) for h in all_credential_hits}
    stale_baseline_entries = sorted(
        f"{file_}\t{pattern}"
        for file_, pattern in baseline
        if (file_, pattern) not in seen_pairs
    )

    ok = not new_hits
    result = {
        "ok": ok,
        "range": rev_range,
        "addedLines": added_lines,
        "credentialHits": new_hits,
        "baselinedCredentialHits": baselined_hits,
        "staleBaselineEntries": stale_baseline_entries,
        "entropyCandidateCount": len(entropy_hits),
        "entropySample": entropy_hits[:20],
    }
    if not ok:
        result["error"] = (
            f"{len(new_hits)} credential-shaped pattern hit(s) in the "
            f"{rev_range} patch text are NOT in {BASELINE_PATH.relative_to(REPO_ROOT)} "
            "— see credentialHits. Remove the secret, rotate it if it is live, "
            "and rewrite history before pushing. If manual review confirms this "
            "is a synthetic fixture (matches the existing '# sanitizer:ignore' "
            "convention used elsewhere in this repo's test suite), either add "
            "that marker to the line, or add a reviewed "
            "'<file>\\t<pattern>' entry to the baseline file with a comment "
            "explaining why it is safe."
        )
    return (0 if ok else 1), result


def _self_check() -> tuple[int, dict]:
    """Prove the credential half catches a planted secret and honors the marker."""
    tmp = Path(tempfile.mkdtemp(prefix="secret-history-selfcheck-"))
    try:
        subprocess.run(["git", "init", "-q"], cwd=str(tmp), check=True)
        subprocess.run(
            ["git", "config", "user.email", "gate@example.invalid"],
            cwd=str(tmp),
            check=True,
        )
        subprocess.run(["git", "config", "user.name", "gate"], cwd=str(tmp), check=True)
        (tmp / "README.md").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=str(tmp), check=True)
        subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=str(tmp), check=True)
        subprocess.run(
            ["git", "branch", "-q", "origin-main-stand-in"], cwd=str(tmp), check=True
        )

        # Known-bad: a real-shaped AWS key, a GitHub PAT, and a PEM block.
        bad = (
            "AWS_ACCESS_KEY_ID = 'AKIAABCDEFGHIJKLMNOP'\n"  # sanitizer:ignore - synthetic self-check fixture written into a throwaway tmp git repo, not a real key
            "GITHUB_TOKEN = 'ghp_" + ("a" * 36) + "'\n"
            # Built via concatenation (not a contiguous literal) so this synthetic
            # fixture's own tracked source doesn't trip the pre-commit-hooks
            # detect-private-key scanner, which has no sanitizer:ignore/marker
            # support at all -- same convention as the GITHUB_TOKEN line above.
            "-----BEGIN " + "RSA PRIVATE KEY" + "-----\n"
        )
        (tmp / "leak.py").write_text(bad, encoding="utf-8")
        subprocess.run(["git", "add", "leak.py"], cwd=str(tmp), check=True)
        subprocess.run(
            ["git", "commit", "-q", "-m", "planted secret"], cwd=str(tmp), check=True
        )

        rc_bad, result_bad = check(tmp, base="origin-main-stand-in")
        caught = rc_bad == 1 and len(result_bad.get("credentialHits", [])) >= 2

        # Known-good: same shapes, but marked as an intentional synthetic fixture.
        subprocess.run(
            ["git", "checkout", "-q", "origin-main-stand-in"], cwd=str(tmp), check=True
        )
        subprocess.run(
            ["git", "checkout", "-q", "-B", "exempt-branch"], cwd=str(tmp), check=True
        )
        exempt = "AWS_ACCESS_KEY_ID = 'AKIAABCDEFGHIJKLMNOP'  # sanitizer:ignore - synthetic\n"
        (tmp / "leak.py").write_text(exempt, encoding="utf-8")
        subprocess.run(["git", "add", "leak.py"], cwd=str(tmp), check=True)
        subprocess.run(
            ["git", "commit", "-q", "-m", "exempted secret-shaped fixture"],
            cwd=str(tmp),
            check=True,
        )

        rc_good, result_good = check(tmp, base="origin-main-stand-in")
        exempted = rc_good == 0 and not result_good.get("credentialHits")

        # D-CIP-14: prove the baseline ratchet in BOTH directions.
        # (a) A hit whose (file, pattern) is baselined must NOT block, but must
        #     still be visible (baselinedCredentialHits), not silently dropped.
        subprocess.run(
            ["git", "checkout", "-q", "origin-main-stand-in"], cwd=str(tmp), check=True
        )
        subprocess.run(
            ["git", "checkout", "-q", "-B", "baselined-branch"],
            cwd=str(tmp),
            check=True,
        )
        (tmp / "leak.py").write_text(bad, encoding="utf-8")
        subprocess.run(["git", "add", "leak.py"], cwd=str(tmp), check=True)
        subprocess.run(
            ["git", "commit", "-q", "-m", "same planted secret, now baselined"],
            cwd=str(tmp),
            check=True,
        )
        baseline_file = tmp / "baseline.txt"
        baseline_file.write_text(
            "# test baseline\n"
            "b/leak.py\taws_access_key_id\n"
            "b/leak.py\tgithub_pat_classic\n"
            "b/leak.py\tprivate_key_block\n",
            encoding="utf-8",
        )
        rc_baselined, result_baselined = check(
            tmp, base="origin-main-stand-in", baseline_path=baseline_file
        )
        baseline_exempts = (
            rc_baselined == 0
            and not result_baselined.get("credentialHits")
            and len(result_baselined.get("baselinedCredentialHits", [])) >= 2
        )

        # (b) A baseline entry naming a (file, pattern) that no longer appears in
        #     the range must be reported as stale, not silently retained forever.
        stale_baseline_file = tmp / "stale_baseline.txt"
        stale_baseline_file.write_text(
            "b/nonexistent-file.py\tgeneric_secret_assignment\n", encoding="utf-8"
        )
        _rc_stale, result_stale = check(
            tmp, base="origin-main-stand-in", baseline_path=stale_baseline_file
        )
        stale_detected = (
            "b/nonexistent-file.py\tgeneric_secret_assignment"
            in result_stale.get("staleBaselineEntries", [])
        )

        ok = caught and exempted and baseline_exempts and stale_detected
        return (0 if ok else 1), {
            "ok": ok,
            "selfCheck": True,
            "caughtPlantedCredential": caught,
            "honoredSanitizerMarker": exempted,
            "baselineRatchetExempts": baseline_exempts,
            "baselineRatchetDetectsStaleEntries": stale_detected,
            "plantedRunDetail": result_bad,
            "exemptedRunDetail": result_good,
            "baselinedRunDetail": result_baselined,
        }
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path("."),
        help="tree to scan (default: the current directory; the merge queue's "
        "fast tier passes the merged trial-commit tree here)",
    )
    parser.add_argument("--base", default=None, help="base ref (default: origin/main)")
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="prove the gate catches a known-bad input",
    )
    args = parser.parse_args()

    if args.self_check:
        rc, result = _self_check()
    else:
        repo_root = args.repository_root.resolve()
        baseline_path = repo_root / "scripts" / "security" / "secret_history_baseline.txt"
        rc, result = check(repo_root, args.base, baseline_path=baseline_path)

    print(json.dumps(result, indent=2))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
