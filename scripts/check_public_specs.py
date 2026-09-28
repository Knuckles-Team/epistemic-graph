"""Validate tracked public owner specifications without network or workspace inputs."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

REQUIRED = ("spec.md", "plan.md", "test-spec.md", "tasks.md", "status.json")
DELIVERY_STATES = frozenset(
    {
        "UNKNOWN",
        "SPECIFIED",
        "BUILDING",
        "BUILT",
        "LANDED",
        "CLOSED",
        "DEFERRED",
        "REJECTED",
    }
)
ACCEPTANCE_STATES = frozenset({"NOT_AUDITED", "PENDING", "ACCEPTED", "FAILED"})
EVIDENCE_KINDS = frozenset(
    {
        "merged_head",
        "implementation",
        "branch",
        "pr",
        "test",
        "consumer",
        "release",
        "decision",
    }
)
FORBIDDEN = (
    re.compile(r"plans/", re.IGNORECASE),
    re.compile(r"\bgitlab\b", re.IGNORECASE),
    re.compile(r"\bhomelab\b", re.IGNORECASE),
    re.compile(r"(?:file://|/(?:home|Users|tmp|workspace)/)", re.IGNORECASE),
)
LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
LOCAL_HOST = re.compile(
    r"(?:^localhost$|\.local$|\.internal$|\.lan$|^127\.|^10\.|^192\.168\.)",
    re.IGNORECASE,
)
PLACEHOLDER = re.compile(
    r"\[(?:Describe|Exact|Record|ID|Feature name|repository name|Observable behavior|Measurable)",
    re.IGNORECASE,
)
CONTENT_MARKERS = {
    "spec.md": re.compile(r"requirement|acceptance|\bFR-\d+\b", re.IGNORECASE),
    "plan.md": re.compile(r"architecture|design|interface", re.IGNORECASE),
    "test-spec.md": re.compile(r"test|expected|proof", re.IGNORECASE),
    "tasks.md": re.compile(r"(?m)^- \[[ xX]\]"),
}


def tracked_specs(root: Path) -> list[Path]:
    return [
        path.relative_to(root) for path in (root / "specs").rglob("*") if path.is_file()
    ]


def _document_errors(path: Path, content: str) -> list[str]:
    errors = []
    if len(content.strip()) < 250:
        errors.append(f"{path}: add substantive design or proof")
    if PLACEHOLDER.search(content):
        errors.append(f"{path}: unresolved template placeholder")
    if not CONTENT_MARKERS[path.name].search(content):
        errors.append(f"{path}: missing required behavior, design, test, or tasks")
    return errors


def _evidence_errors(path: Path, entries: object) -> list[str]:
    if not isinstance(entries, list):
        return [f"{path}: evidence must be an array"]
    errors = []
    for item in entries:
        if not isinstance(item, dict):
            errors.append(f"{path}: evidence item must be an object")
            continue
        errors.extend(_evidence_item_errors(path, item))
    return errors


def _evidence_item_errors(path: Path, item: dict) -> list[str]:
    errors = []
    url = item.get("url", "")
    commit = item.get("commit", "")
    if item.get("kind") not in EVIDENCE_KINDS:
        errors.append(f"{path}: unsupported evidence kind")
    if not isinstance(url, str) or not url.startswith("https://github.com/"):
        errors.append(f"{path}: evidence requires a public GitHub URL")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        errors.append(f"{path}: evidence requires a full source commit")
    if item.get("result") not in ("passed", "failed") or not item.get("description"):
        errors.append(f"{path}: evidence requires a result and description")
    return errors


def _passed_receipt(entries: object, kind: str, commit: str) -> bool:
    return isinstance(entries, list) and any(
        isinstance(item, dict)
        and item.get("kind") == kind
        and item.get("result") == "passed"
        and item.get("commit") == commit
        for item in entries
    )


def _merged_head(entries: object, owner_repo: str) -> str | None:
    if not isinstance(entries, list):
        return None
    for item in entries:
        if not isinstance(item, dict) or item.get("kind") != "merged_head":
            continue
        commit = item.get("commit", "")
        url = f"https://github.com/Knuckles-Team/{owner_repo}/commit/{commit}"
        if item.get("result") == "passed" and item.get("url") == url:
            return commit
    return None


def _receipt_errors(path: Path, data: dict, entries: object) -> list[str]:
    commit = _merged_head(entries, data.get("owner_repo", ""))
    errors = []
    if data.get("delivery_state") in ("LANDED", "CLOSED") and not commit:
        errors.append(f"{path}: LANDED/CLOSED requires merged-head evidence")
    if data.get("acceptance_state") == "ACCEPTED" and not (
        commit and _passed_receipt(entries, "test", commit)
    ):
        errors.append(f"{path}: ACCEPTED requires merged-head and test evidence")
    if data.get("acceptance_state") == "ACCEPTED" and not (
        commit
        and (
            _passed_receipt(entries, "consumer", commit)
            or _passed_receipt(entries, "release", commit)
        )
    ):
        errors.append(f"{path}: ACCEPTED requires consumer or release evidence")
    return errors


def _status_errors(root: Path, path: Path) -> list[str]:
    try:
        data = json.loads((root / path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [f"{path}: invalid JSON ({exc})"]
    if not isinstance(data, dict):
        return [f"{path}: status must be an object"]
    entries = data.get("evidence")
    return (
        _status_field_errors(path, data)
        + _evidence_errors(path, entries)
        + _receipt_errors(path, data, entries)
    )


def _status_field_errors(path: Path, data: dict) -> list[str]:
    errors = []
    ids = data.get("requirement_ids")
    if (
        data.get("schema_version") != 1
        or not data.get("spec_id")
        or not data.get("owner_repo")
    ):
        errors.append(f"{path}: schema_version, spec_id, and owner_repo are required")
    if not _valid_ids(ids):
        errors.append(f"{path}: nonempty real requirement_ids are required")
    if data.get("delivery_state") not in DELIVERY_STATES:
        errors.append(f"{path}: invalid delivery_state")
    if data.get("acceptance_state") not in ACCEPTANCE_STATES:
        errors.append(f"{path}: invalid acceptance_state")
    return errors


def _valid_ids(ids: object) -> bool:
    return (
        isinstance(ids, list)
        and bool(ids)
        and all(isinstance(item, str) and bool(item) for item in ids)
    )


def _contract_errors(root: Path, paths: set[Path]) -> list[str]:
    errors = []
    names = sorted(
        {
            path.parts[1]
            for path in paths
            if len(path.parts) > 2 and path.parts[1] != "_template"
        }
    )
    for name in names:
        for filename in REQUIRED:
            path = Path("specs") / name / filename
            if path not in paths:
                errors.append(f"{path}: missing required owner contract")
                continue
            if filename == "status.json":
                errors.extend(_status_errors(root, path))
            else:
                errors.extend(
                    _document_errors(path, (root / path).read_text(encoding="utf-8"))
                )
    return errors


def _link_error(root: Path, path: Path, target: str) -> str | None:
    target = target.strip().split(" ", 1)[0].strip("<>")
    if target.startswith("#"):
        return None
    parsed = urlsplit(target)
    if parsed.scheme:
        public = parsed.scheme in ("http", "https") and parsed.hostname
        if not public or LOCAL_HOST.search(parsed.hostname or ""):
            return f"{path}: non-public link {target}"
        return None
    if target.startswith("/"):
        return f"{path}: absolute local link {target}"
    candidate = (root / path.parent / unquote(parsed.path)).resolve()
    if not candidate.is_relative_to(root.resolve()) or not candidate.exists():
        return f"{path}: broken relative link {target}"
    return None


def _reference_errors(root: Path, path: Path) -> list[str]:
    content = (root / path).read_text(encoding="utf-8")
    errors: list[str] = []
    if "_template" not in path.parts:
        errors.extend(
            f"{path}: private or local reference ({pattern.pattern})"
            for pattern in FORBIDDEN
            if pattern.search(content)
        )
    errors.extend(
        issue
        for target in LINK.findall(content)
        if (issue := _link_error(root, path, target))
    )
    return errors


def problems(root: Path) -> list[str]:
    paths = set(tracked_specs(root))
    errors = _contract_errors(root, paths)
    for path in sorted(paths):
        if path.suffix == ".md":
            errors.extend(_reference_errors(root, path))
    return errors


def main() -> int:
    errors = problems(Path(__file__).resolve().parents[1])
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Public specs are self-contained and locally linked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
