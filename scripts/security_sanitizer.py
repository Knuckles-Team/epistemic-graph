#!/usr/bin/env python3
import os
import re
import subprocess
import sys
from pathlib import Path

MAX_SCAN_BYTES = 8 * 1024 * 1024

# Config
# llms.txt is the deliberate root-level AI entry index (llms-txt convention,
# like robots.txt) shipped by the docs/day-0 work — not garbage.
# overrides.txt is the sanctioned uv dependency-override file (UV_OVERRIDE in
# docker/Dockerfile, mirroring [tool.uv] override-dependencies) shipped by the
# pydantic-ai v2 migration — a canonical packaging input, not scratch.
# .security-audit-allow.txt is the OSV dependency-audit risk-acceptance ledger
# (scripts/audit_dependencies.py, wired into the dependency-audit pre-commit hook)
# — a committed, actively-read governance input, not scratch/garbage.
# .cargo-audit-allow.txt is its Rust twin: the RUSTSEC advisory risk-acceptance
# ledger (deny.toml / cargo-deny CVE gate) — same governance role, not garbage.
ALLOWED_TXT_NAMES = {
    "requirements.txt",
    "requirements-dev.txt",
    ".cargo-audit-allow.txt",
    "llms.txt",
    "overrides.txt",
    ".security-audit-allow.txt",
}
TRANSIENT_PY_PATTERNS = [
    re.compile(r"^test_.*\.py$"),
    re.compile(r"^fix_.*\.py$"),
    re.compile(r"^debug_.*\.py$"),
    re.compile(r"^scratch_.*\.py$"),
    re.compile(r"^temp_.*\.py$"),
]
TRANSIENT_NOTE_PATTERNS = [
    re.compile(r".*[-_ ]BASELINE[-_ ]NOTES\.md$", re.IGNORECASE),
    re.compile(r".*[-_ ]WORKING[-_ ]NOTES\.md$", re.IGNORECASE),
    re.compile(r".*[-_ ]HANDOFF[-_ ]NOTES\.md$", re.IGNORECASE),
    re.compile(r".*[-_ ]SCRATCHPAD\.md$", re.IGNORECASE),
]

SECRET_PATTERNS = [
    ("GitHub PAT", re.compile(r"ghp_[A-Za-z0-9_]{36,255}")),
    ("GitHub Fine-grained PAT", re.compile(r"github_pat_[A-Za-z0-9_]{82,255}")),
    ("GitLab PAT", re.compile(r"glpat-[A-Za-z0-9\-]{20,255}")),
    ("Private key", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
    ("AWS access key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("Slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    ("Langfuse secret", re.compile(r"\bsk-lf-[A-Za-z0-9_-]{16,}\b")),
    (
        "Generic Secret Assignment",
        re.compile(
            r"secret[A-Za-z0-9_]*\s*[:=]\s*['\"][A-Za-z0-9_\-\.\~\*]{16,255}['\"]",
            re.IGNORECASE,
        ),
    ),
    (
        "Generic Token Assignment",
        re.compile(
            r"token\s*[:=]\s*['\"][A-Za-z0-9_\-\.\~\*]{16,255}['\"]", re.IGNORECASE
        ),
    ),
]

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "build",
    "dist",
    "__pycache__",
    ".tox",
    ".specify",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".cache",
}
EXCLUDED_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".pyc",
    ".db",
    ".kuzu",
    ".sqlite",
    ".sqlite3",
    ".zip",
    ".tar.gz",
    ".tgz",
    ".bz2",
    ".xz",
    ".pdf",
    ".bin",
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".woff",
    ".woff2",
    ".eot",
    ".ttf",
    ".mp4",
    ".mp3",
    ".wav",
    ".lock",
    ".svg",
}

# Placeholder / Mock indicators
PLACEHOLDER_VALUES = {
    "1234567890",
    "abcdef12345",
    "abc123youandme",
    "askdfalskdvjas",
    "test_token",
    "test_secret",
    "glpat-askdfalskdvjas",
    "github_pat_12345",
    "glpat-abc123youandme",
    "github_pat_...",
    "glpat-*************",
    "ghp_*************",
    "github_pat_*************",
    "token_*************",
    "secret_*************",
    "glpat-abc",
    "ghp_abc",
    "github_pat_abc",
    "${env:",
}
PLACEHOLDER_PREFIXES = (
    "your_",
    "your-",
    "dummy",
    "example",
    "mock",
    "test_",
    "test-",
    "synthetic_",
    "synthetic-",
)


#: Inline exemption marker, shared with
#: ``scripts/security/check_secret_history.py``. A line carrying it is skipped by
#: the credential patterns below. ONE convention for both scanners, so a
#: reviewed synthetic fixture can be marked safe once rather than needing a
#: different mechanism per tool.
SANITIZER_IGNORE_MARKER = "sanitizer:ignore"


def is_placeholder(match_str: str) -> bool:
    # Generic assignment patterns include the variable name. Judge only the
    # quoted value so names such as ``secret_example`` cannot suppress a real
    # credential finding.
    assignment = re.search(r"['\"]([^'\"]+)['\"]\s*$", match_str)
    candidate = assignment.group(1) if assignment else match_str
    candidate_lower = candidate.strip().lower()
    if candidate_lower in PLACEHOLDER_VALUES or candidate_lower.startswith(
        PLACEHOLDER_PREFIXES
    ):
        return True

    # Check if match is mostly asterisks or single repeated char
    cleaned = candidate.replace("'", "").replace('"', "").strip()
    if not cleaned:
        return True

    # Check if there are sequences of asterisks indicating masked values
    if "*" in cleaned:
        # e.g., glpat-*************
        return True

    compact = re.sub(r"[^A-Za-z0-9]", "", cleaned).lower()
    if len(compact) >= 8 and len(set(compact)) == 1:
        return True

    return False


def get_repo_files(repo_path: Path):
    try:
        result = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        files = []
        for line in result.stdout.splitlines():
            if line.strip():
                relative = Path(line.strip())
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("unsafe source inventory path")
                # Avoid files inside excluded directories
                parts = relative.parts
                if not any(part in EXCLUDED_DIRS for part in parts):
                    files.append(repo_path / relative)
        return files
    except (OSError, subprocess.SubprocessError, ValueError):
        # Fallback to manual recursive scan
        files = []
        for root, dirs, walk_files in os.walk(str(repo_path)):
            # Hidden source/config directories (for example ``.github``) can
            # contain credentials and must not disappear merely because Git
            # inventory was unavailable. Exclude only known generated trees.
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
            for file in walk_files:
                files.append(Path(root) / file)
        return files


def scan_repository(repo_path: Path):
    violations = []
    files_to_scan = get_repo_files(repo_path)

    for file_path in files_to_scan:
        if not file_path.is_file():
            continue
        if file_path.is_symlink():
            # Git tracks the link target text, not the target contents. Never
            # follow a repository symlink into machine-local material.
            continue

        for pattern in TRANSIENT_NOTE_PATTERNS:
            if pattern.fullmatch(file_path.name):
                violations.append(
                    "Transient agent note detected: "
                    f"'{file_path.relative_to(repo_path)}'. Keep durable decisions "
                    "in canonical documentation and scratch notes outside the repository."
                )
                break

        # 1. Check root level naming constraints
        if file_path.parent == repo_path:
            # Check txt files
            if file_path.suffix == ".txt":
                if file_path.name.lower() not in ALLOWED_TXT_NAMES:
                    violations.append(
                        "Non-standard root-level text file detected: "
                        f"'{file_path.name}'. Only {sorted(ALLOWED_TXT_NAMES)} "
                        "are allowed."
                    )
            # Check transient py files
            elif file_path.suffix == ".py":
                for pattern in TRANSIENT_PY_PATTERNS:
                    if pattern.match(file_path.name):
                        violations.append(
                            "Transient/temporary script detected in root: "
                            f"'{file_path.name}'. Please move it to a subfolder "
                            "or delete it."
                        )
                        break

        # 2. Check for secrets
        if file_path.suffix.lower() in EXCLUDED_EXTENSIONS:
            continue

        if file_path.name == "security_sanitizer.py":
            continue

        try:
            if file_path.stat().st_size > MAX_SCAN_BYTES:
                violations.append(
                    f"Source file exceeds security scan boundary: "
                    f"'{file_path.relative_to(repo_path)}'"
                )
                continue
            # Decode strictly. Silently discarding invalid bytes can splice a
            # credential around the discarded byte and make a fail-open scan.
            content = file_path.read_text(encoding="utf-8")
            lines = content.splitlines()

            for idx, line in enumerate(lines, 1):
                # Honour the SAME inline exemption marker this repo's other
                # credential scanner (`scripts/security/check_secret_history.py`)
                # already honours, and which the test suite already uses.
                #
                # Before this, the two scanners disagreed: a synthetic fixture
                # correctly marked `# sanitizer:ignore` for one still tripped the
                # other, so there was no way to mark a fixture safe for both. The
                # sharpest example is check_secret_history.py's OWN self-check
                # fixtures — the planted AWS key it uses to prove it still detects
                # a real credential — which carried the marker and were flagged
                # here anyway.
                #
                # Deliberately a LINE marker, not a value allowlist: it forces the
                # exemption to sit next to the literal it exempts, where review
                # sees it, instead of in a distant list that silently widens.
                if SANITIZER_IGNORE_MARKER in line:
                    continue
                for label, pattern in SECRET_PATTERNS:
                    for match in pattern.findall(line):
                        match_str = match[0] if isinstance(match, tuple) else match
                        if not is_placeholder(match_str):
                            rel_path = file_path.relative_to(repo_path)
                            violations.append(
                                f"Potential unmasked secret ({label}) detected in "
                                f"{rel_path}:{idx}"
                            )
        except (OSError, UnicodeError):
            violations.append(
                f"Source file could not be inspected: "
                f"'{file_path.relative_to(repo_path)}'"
            )

    return violations


def main():
    repo_path = Path.cwd()

    print("🔒 Running Security and Garbage Sanitizer...")
    violations = scan_repository(repo_path)

    if violations:
        print("\n❌ SECURITY AND GARBAGE VALIDATION FAILED!")
        print("Please correct the following issues before committing:")
        for idx, violation in enumerate(violations, 1):
            print(f"\n[{idx}] {violation}")
        sys.exit(1)

    print("✅ All checks passed! No root garbage or unmasked secrets detected.")
    sys.exit(0)


if __name__ == "__main__":
    main()
