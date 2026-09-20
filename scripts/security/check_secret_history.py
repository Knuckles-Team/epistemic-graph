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
   that actually blocks a push. **There is no allowlist file, and no
   pattern-level or file-level exemption.** The only exemptions are (a) the
   documented ``# sanitizer:ignore`` marker on the offending line itself
   (matches the convention already used by this repo's synthetic
   credential-shaped test fixtures), and (b), for a hit whose exact line was
   already committed before this gate could catch it and cannot be marked
   after the fact — see "Reviewed exceptions" below — a single, narrowly
   pinned entry in ``secret_history_reviewed_exceptions.toml``. Both are
   forward-looking-strict: any OTHER secret-shaped match, in any other
   commit/file/line, fails, full stop.
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

**No baseline / allowlist file (go-forward fix, 2026-08-27).** This gate used
to carry a ``secret_history_baseline.txt`` allowlist of ``(file, pattern)``
pairs seeded once, by hand, against a 300-commit sample of real history
during the release.yml/advisory.yml CI redesign. Per the NO RATCHETS policy
(baseline/ratchet gates are not allowed here) and the decision to fix secrets
going forward without rewriting already-pushed git history, the baseline is
retired: every seeded entry was confirmed to have EITHER (a) no live match
against the current working tree (the source changed, or the file was
deleted, since the baseline was written), or (b) a match that is a synthetic
test/fixture value, never a real credential. Removing the file changed the
gate's verdict for **zero** currently-scanned lines (this repo's
``origin/main..HEAD`` scans clean before and after). Going forward, EVERY
credential-shaped hit in the scanned range is a hard failure unless the line
itself carries ``# sanitizer:ignore``.

**Reviewed exceptions for immutable pre-push history (EH-313, operator
ruling, 2026-09-19).** ``# sanitizer:ignore`` only works going forward: it
must be added to the offending line, and this repo's own already-landed
commits cannot be edited from a downstream branch without rewriting history
that every recorded checkpoint/ledger SHA and every live lane depends on.
When one specific historical hit is confirmed, by a human, not to be a live
secret, and rewriting that commit is a materially worse outcome than
recording the exception, ``secret_history_reviewed_exceptions.toml``
(same directory as this file) may carry ONE entry for it — modeled directly
on ``dupehound-distinct.toml``'s precedent (EH-264/EH-268): each entry pins
the EXACT commit, file, pattern name, AND a sha256 digest of the exact
matched line text, plus a written ``reason`` a reviewer can check against
the actual commit. An entry matches ONE finding only — not the file, not the
pattern globally, and never the gate as a whole; a different hit of the same
pattern (a different commit, a different file, or even the same file's next
commit) is unaffected and still fails. This is not the retired baseline
reborn: the baseline was a bulk allowlist seeded once by hand and covering
whatever it happened to match; this file requires one reviewed entry per
finding, with the digest pin meaning any change to the underlying content
invalidates the entry and the finding returns. See that file's own header
for the exact schema and the one entry it currently carries.

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
``scripts/security/check_*.py`` contract check carries, for parity with the
convention agent-utilities' sibling scanner uses). This repo wires the gate
into ``.github/workflows/release.yml`` (``python3 scripts/security/
check_secret_history.py --base "$BASE"``), not pre-commit or the merge
queue — the repo root defaults to the current directory, which is correct
for that CI invocation; pass ``--repository-root`` explicitly only when
scanning a checkout other than the current directory (e.g. a merged-tree
verification run).

``--self-check`` proves the credential-pattern half actually catches a
planted secret (AWS-shaped key, GitHub PAT, private-key block) in a throwaway
git repo, that the ``sanitizer:ignore`` marker exempts it, and that a
reviewed-exception entry exempts ONLY the exact (commit, file, pattern,
content-digest) it names — a second, differently-shaped hit of the very same
pattern in the same throwaway repo is still caught, proving the mechanism
cannot be widened into a pattern- or file-level bypass by construction —
i.e. it would have caught a real leak, not just describe one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import tomllib

REPO_ROOT = Path(__file__).resolve().parents[2]

SANITIZER_MARKER = "sanitizer:ignore"

# EH-313 (operator ruling, 2026-09-19): a narrow, per-finding reviewed
# exception file for a hit whose exact historical line cannot be marked with
# `# sanitizer:ignore` after the fact (the commit already landed before this
# gate existed to catch it). See the module docstring's "Reviewed exceptions"
# section and this file's own header for the schema and required argument.
REVIEWED_EXCEPTIONS_FILENAME = "secret_history_reviewed_exceptions.toml"
_REQUIRED_EXCEPTION_KEYS = frozenset(
    {"commit", "file", "pattern", "context_digest", "reviewed_on", "reason"}
)


def _context_digest(context: str) -> str:
    """Content pin for one matched line, mirroring dupehound-distinct.toml's
    function-digest pin: if the underlying text changes at all, the digest
    changes, the exception stops matching, and the finding returns."""
    return "sha256:" + hashlib.sha256(context.encode("utf-8")).hexdigest()


def _load_reviewed_exceptions(repo_root: Path) -> list[dict]:
    """Load the reviewed-exception register, or ``[]`` if none is tracked.

    Every entry must carry all of ``_REQUIRED_EXCEPTION_KEYS`` — a malformed
    entry is a hard error (fail closed), never silently ignored, since a
    typo'd field would otherwise make an intended exception quietly not
    apply (harmless) or, worse, mask a validation bug that could someday let
    one apply too broadly.
    """
    path = repo_root / "scripts" / "security" / REVIEWED_EXCEPTIONS_FILENAME
    if not path.is_file():
        return []
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    entries = data.get("exception", [])
    for entry in entries:
        missing = _REQUIRED_EXCEPTION_KEYS - entry.keys()
        if missing:
            raise ValueError(
                f"{path}: exception entry missing required key(s) {sorted(missing)}: "
                f"{entry!r}"
            )
    return entries


def _apply_reviewed_exceptions(
    hits: list[dict], exceptions: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Split ``hits`` into (still-blocking, exempted-with-record).

    A hit is exempted ONLY when an exception entry matches its commit, file,
    pattern name, AND a sha256 digest of its exact context text — all four,
    exactly. This is a one-finding-at-a-time match, never a (file) or
    (pattern) wildcard: an exception for one commit's hit has no effect on
    any other commit, even one that hits the very same pattern in the very
    same file.
    """
    blocking: list[dict] = []
    exempted: list[dict] = []
    for hit in hits:
        digest = _context_digest(hit["context"])
        match = next(
            (
                entry
                for entry in exceptions
                if entry["commit"] == hit["commit"]
                and entry["file"] == hit["file"]
                and entry["pattern"] == hit["pattern"]
                and entry["context_digest"] == digest
            ),
            None,
        )
        if match is None:
            blocking.append(hit)
        else:
            exempted.append(
                {
                    **hit,
                    "reviewedOn": match["reviewed_on"],
                    "reason": match["reason"],
                }
            )
    return blocking, exempted


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


#: This scanner's own source necessarily CONTAINS the credential shapes it
#: hunts for -- `aws_access_key_id`, `private_key_block` and friends are regex
#: literals here. Scanning them flags the DETECTOR as the leak: on its very
#: first queue run this gate rejected its OWN branch with 5 "credential-shaped"
#: hits, every one of them its own pattern table.
#:
#: Excluding it is not a weakening. A scanner cannot meaningfully audit itself
#: -- any finding is by construction a definition, not a secret -- and both
#: `check_tracked_privacy.py` and `check_wheel_privacy.py` already exclude
#: `scripts/` for exactly this reason ("the scanners live there").
#:
#: Scoped to THIS FILE ONLY, deliberately: a blanket `scripts/security/`
#: exclusion would blind the gate to a real credential committed into any other
#: gate in that directory.
_SELF_PATHS = ("scripts/security/check_secret_history.py",)


def _is_self(file_: str) -> bool:
    """True for this scanner's own source (git patch `b/` prefix)."""
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


def _high_entropy_tokens(content: str) -> list[tuple[str, float]]:
    """Tokens in one added line that look like real random credential material.

    A candidate must survive all three filters -- known-noise classifier, three
    distinct character classes, entropy floor. Any one alone is mostly hashes.
    """
    found = []
    for match in _ENTROPY_TOKEN_RE.finditer(content):
        token = match.group(0)
        if _is_entropy_noise(token):
            continue
        classes = sum(
            bool(re.search(p, token))
            for p in (r"[a-z]", r"[A-Z]", r"[0-9]", r"[+/_.=-]")
        )
        if classes < 3:
            continue
        entropy = _shannon_entropy(token)
        if entropy >= _MIN_ENTROPY:
            found.append((token, entropy))
    return found


def _is_lockfile(file_: str) -> bool:
    """True for a dependency lockfile, whose hashes are entropy noise."""
    return file_ != "?" and file_.endswith(_LOCKFILE_SUFFIXES)


def scan_entropy(patch_lines: list[str]) -> list[dict]:
    hits: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for commit, file_, content in _iter_added_lines(patch_lines):
        if _is_self(file_) or _is_lockfile(file_):
            continue
        for token, entropy in _high_entropy_tokens(content):
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


def _ref_exists(repo: Path, ref: str) -> bool:
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", ref],
        cwd=str(repo),
        capture_output=True,
        check=False,
    )
    return proc.returncode == 0


def check(repo_root: Path, base: str | None) -> tuple[int, dict]:
    """Absolute, forward-looking gate — no allowlist file (2026-08-27).

    Every credential-shaped hit in ``<base>..HEAD`` is a hard failure. The
    only exemptions are the inline ``# sanitizer:ignore`` marker on the
    offending line itself, and a reviewed, narrowly-pinned entry in
    ``secret_history_reviewed_exceptions.toml`` for a hit whose exact
    historical line predates this gate and cannot be marked after the fact
    (EH-313) — see ``_apply_reviewed_exceptions``. Neither is a
    ``(file, pattern)`` baseline that can widen or go stale: each covers
    exactly one already-matched finding, identified by content digest.
    """
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
    raw_credential_hits = scan_credentials(patch_lines)
    reviewed_exceptions = _load_reviewed_exceptions(repo_root)
    credential_hits, exempted_hits = _apply_reviewed_exceptions(
        raw_credential_hits, reviewed_exceptions
    )
    entropy_hits = scan_entropy(patch_lines)

    ok = not credential_hits
    result = {
        "ok": ok,
        "range": rev_range,
        "addedLines": added_lines,
        "credentialHits": credential_hits,
        # Always present, even when empty, so an exception being applied is
        # visible in every run's output, not just discoverable by diffing.
        "reviewedExceptions": exempted_hits,
        "entropyCandidateCount": len(entropy_hits),
        "entropySample": entropy_hits[:20],
    }
    if not ok:
        result["error"] = (
            f"{len(credential_hits)} credential-shaped pattern hit(s) in the "
            f"{rev_range} patch text — see credentialHits. There is no "
            "allowlist file for this gate. Remove the secret and rotate it if "
            "it is live (this is a go-forward fix — already-pushed history is "
            "not rewritten), or, if manual review confirms this is a "
            "synthetic fixture, add the inline '# sanitizer:ignore' marker to "
            "that exact line, or, only for a historical line that already "
            "landed before this gate existed, add one reviewed, digest-pinned "
            "entry to secret_history_reviewed_exceptions.toml (EH-313) — the "
            "only exemptions this gate honors."
        )
    return (0 if ok else 1), result


def _self_check_init_repo(tmp: Path) -> None:
    """Base commit + ``origin-main-stand-in`` branch every scenario forks from."""
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


def _self_check_credential_and_marker(tmp: Path) -> dict:
    """Scenarios 1-2: a planted secret is caught; ``sanitizer:ignore`` exempts it
    (MR-6: a gate nobody proved catches a violation is not a gate)."""
    # Known-bad: a real-shaped AWS key, a GitHub PAT, and a PEM block.
    bad = (
        "AWS_ACCESS_KEY_ID = 'AKIAABCDEFGHIJKLMNOP'\n"  # sanitizer:ignore - fixture
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
    exempt = "AWS_ACCESS_KEY_ID = 'AKIAABCDEFGHIJKLMNOP'  # sanitizer:ignore - "
    "synthetic\n"
    (tmp / "leak.py").write_text(exempt, encoding="utf-8")
    subprocess.run(["git", "add", "leak.py"], cwd=str(tmp), check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "exempted secret-shaped fixture"],
        cwd=str(tmp),
        check=True,
    )

    rc_good, result_good = check(tmp, base="origin-main-stand-in")
    exempted = rc_good == 0 and not result_good.get("credentialHits")

    return {
        "caught": caught,
        "exempted": exempted,
        "plantedRunDetail": result_bad,
        "exemptedRunDetail": result_good,
    }


def _self_check_write_exception_entry(tmp: Path, hit: dict) -> None:
    """Record one reviewed-exception entry, in the same shape a human would."""
    exceptions_dir = tmp / "scripts" / "security"
    exceptions_dir.mkdir(parents=True, exist_ok=True)
    entry = (
        "[[exception]]\n"
        f'commit = "{hit["commit"]}"\n'
        f'file = "{hit["file"]}"\n'
        f'pattern = "{hit["pattern"]}"\n'
        f'context_digest = "{_context_digest(hit["context"])}"\n'
        'reviewed_on = "2026-09-19"\n'
        'reason = "self-check fixture -- proves the exemption is scoped '
        'to exactly one finding, not the file or pattern."\n'
    )
    (exceptions_dir / REVIEWED_EXCEPTIONS_FILENAME).write_text(entry, encoding="utf-8")
    subprocess.run(
        ["git", "add", "scripts/security/" + REVIEWED_EXCEPTIONS_FILENAME],
        cwd=str(tmp),
        check=True,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "record the reviewed exception"],
        cwd=str(tmp),
        check=True,
    )


def _self_check_reviewed_exception(tmp: Path) -> dict:
    """Scenario 3 (EH-313): an exception exempts ONLY the exact
    (commit, file, pattern, content-digest) it names -- proven two ways: (a)
    the hit it names disappears from credentialHits and shows up in
    reviewedExceptions instead; (b) a SECOND, differently-shaped hit of the
    very same pattern, planted in a later commit, is still caught -- i.e. the
    entry cannot be mistaken for a pattern- or file-level bypass.
    """
    subprocess.run(
        ["git", "checkout", "-q", "origin-main-stand-in"], cwd=str(tmp), check=True
    )
    subprocess.run(
        ["git", "checkout", "-q", "-B", "reviewed-exception-branch"],
        cwd=str(tmp),
        check=True,
    )
    # Built via concatenation, same convention as the fixtures above, so this
    # self-check's own tracked source is not itself a credential-shaped
    # literal outside a controlled throwaway repo.
    first_dsn = "postgres://" + "svc" + ":" + "s3cr3t" + "@db.internal.example.com/prod"
    (tmp / "leak2.py").write_text(f"DB_URL = '{first_dsn}'\n", encoding="utf-8")
    subprocess.run(["git", "add", "leak2.py"], cwd=str(tmp), check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "first planted DSN, to be exempted"],
        cwd=str(tmp),
        check=True,
    )
    _, result_pre = check(tmp, base="origin-main-stand-in")
    planted_hit = next(
        (
            h
            for h in result_pre.get("credentialHits", [])
            if h.get("file", "").endswith("leak2.py")
        ),
        None,
    )
    if planted_hit is not None:
        _self_check_write_exception_entry(tmp, planted_hit)
    rc_exempted, result_exempted = check(tmp, base="origin-main-stand-in")
    applied = (
        planted_hit is not None
        and rc_exempted == 0
        and not result_exempted.get("credentialHits")
        and len(result_exempted.get("reviewedExceptions", [])) == 1
    )

    # Second, DIFFERENT DSN in a later commit -- must NOT be swallowed by the
    # exception entry above.
    second_dsn = "mysql://" + "app" + ":" + "hunter2" + "@10.0.0.9/billing"
    (tmp / "leak3.py").write_text(f"OTHER_DB_URL = '{second_dsn}'\n", encoding="utf-8")
    subprocess.run(["git", "add", "leak3.py"], cwd=str(tmp), check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "second planted DSN, must still be caught"],
        cwd=str(tmp),
        check=True,
    )
    rc_second, result_second = check(tmp, base="origin-main-stand-in")
    second_still_caught = rc_second == 1 and any(
        h.get("file", "").endswith("leak3.py")
        for h in result_second.get("credentialHits", [])
    )

    return {
        "written": planted_hit is not None,
        "applied": applied,
        "scopedNarrowly": applied and second_still_caught,
        "reviewedExceptionRunDetail": result_exempted,
        "secondPlantedHitStillCaughtDetail": result_second,
    }


def _self_check() -> tuple[int, dict]:
    """Prove the credential half catches a planted secret, honors the marker,
    and that a reviewed exception (EH-313) cannot be widened into a
    pattern/file-level bypass. Each scenario is its own function; this is
    only the shared throwaway repo's setup/teardown and the verdict."""
    tmp = Path(tempfile.mkdtemp(prefix="secret-history-selfcheck-"))
    try:
        _self_check_init_repo(tmp)
        marker = _self_check_credential_and_marker(tmp)
        exception = _self_check_reviewed_exception(tmp)
        ok = (
            marker["caught"]
            and marker["exempted"]
            and exception["written"]
            and exception["scopedNarrowly"]
        )
        return (0 if ok else 1), {
            "ok": ok,
            "selfCheck": True,
            "caughtPlantedCredential": marker["caught"],
            "honoredSanitizerMarker": marker["exempted"],
            "reviewedExceptionApplied": exception["applied"],
            "reviewedExceptionScopedNarrowly": exception["scopedNarrowly"],
            "plantedRunDetail": marker["plantedRunDetail"],
            "exemptedRunDetail": marker["exemptedRunDetail"],
            "reviewedExceptionRunDetail": exception["reviewedExceptionRunDetail"],
            "secondPlantedHitStillCaughtDetail": exception[
                "secondPlantedHitStillCaughtDetail"
            ],
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
        rc, result = check(repo_root, args.base)

    print(json.dumps(result, indent=2))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
