#!/usr/bin/env python3
"""Fail-closed OSV audit for the committed ``uv.lock``
(CONCEPT:EG-KG.storage.python-dependency-cve-gate).

This is the Python twin of agent-utilities' own ``scripts/audit_dependencies.py`` --
same file, same conventions, deliberately not reinvented, so EG and AU share one
audit contract. It closes the gap recorded in ``deny.toml`` (commit 3027965e):
``cargo-deny``/``deny.toml`` + ``.cargo-audit-allow.txt`` audit the RUST graph
only, and ``bandit`` is SAST over EG's own source, not an advisory check against
installed packages -- so EG's Python client + wheel had no CVE gate at all. The
ledger here (``.security-audit-allow.txt``) is the Python twin of the Rust side's
``.cargo-audit-allow.txt``, and ``scripts/check_python_advisories.sh`` cross-
validates it the same way ``scripts/check_cargo_advisories.sh`` cross-validates
the Rust ledger against ``deny.toml``: an accepted advisory can never exist
without a live, dated justification, and a justification can never go stale
without failing the gate.

The gate has no project-runtime dependencies.  It parses the lock with
``tomllib``, queries the fixed OSV HTTPS API, bounds every request/response, and
fails for every affected dependency unless an exact advisory/package pair has a
short-lived, justified risk acceptance in ``.security-audit-allow.txt``.

TLS trust remains an environment concern: ``SSL_CERT_FILE`` or
``REQUESTS_CA_BUNDLE`` may point at a complete PEM bundle and ``SSL_CERT_DIR``
may point at a hashed CA directory.  Certificate verification is never disabled.

Offline behavior is fail-closed by default -- the same choice AU made for this
exact script: a scanner that silently passes when it cannot reach the advisory
database is worse than no scanner, and this repo has a documented history of
gates that looked green while enforcing nothing (RF-ADR series). A developer may
explicitly set ``SECURITY_AUDIT_OFFLINE_POLICY=warn`` for a disconnected local
commit, but CI and release certification must leave the default (hard failure)
in place.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import re
import ssl
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

import tomllib

OSV_BATCH = "https://api.osv.dev/v1/querybatch"
OSV_VULN_PREFIX = "https://api.osv.dev/v1/vulns/"
MAX_LOCK_BYTES = 64 * 1024 * 1024
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_PACKAGES = 10_000
MAX_ACCEPTANCE_DAYS = 90
REQUEST_TIMEOUT_SECONDS = 30
ADVISORY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9-]{2,127}$")
PACKAGE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
EXPIRY_RE = re.compile(r"^expires=(\d{4}-\d{2}-\d{2})$")
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class AuditError(RuntimeError):
    """Stable audit failure that omits endpoints, credentials, and local paths."""


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, _newurl):
        raise AuditError("OSV redirect was rejected")


@dataclass(frozen=True)
class RiskAcceptance:
    advisory_id: str
    package: str
    expires: dt.date
    justification: str


def _normalise_package(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _validate_uv_artifacts(item: dict[str, Any]) -> None:
    source = item.get("source")
    if not isinstance(source, dict) or "registry" not in source:
        return
    registry = source.get("registry")
    if not isinstance(registry, str) or not registry.startswith("https://"):
        raise AuditError("dependency lock contains an insecure registry source")
    artifacts: list[Any] = []
    if "sdist" in item:
        artifacts.append(item["sdist"])
    wheels = item.get("wheels", [])
    if not isinstance(wheels, list):
        raise AuditError("dependency lock artifact inventory is invalid")
    artifacts.extend(wheels)
    if not artifacts:
        raise AuditError("dependency lock registry package has no hashed artifact")
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise AuditError("dependency lock artifact inventory is invalid")
        url, digest = artifact.get("url"), artifact.get("hash")
        if (
            not isinstance(url, str)
            or not url.startswith("https://")
            or not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
        ):
            raise AuditError("dependency lock contains an unverified artifact")


def parse_lock(path: pathlib.Path) -> tuple[tuple[str, str], ...]:
    """Return every exact PyPI package/version pair pinned in a bounded uv lock."""

    try:
        size = path.stat().st_size
        if size <= 0 or size > MAX_LOCK_BYTES or path.is_symlink():
            raise AuditError("dependency lock is unavailable or exceeds its safe bound")
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except AuditError:
        raise
    except (OSError, UnicodeError, tomllib.TOMLDecodeError):
        raise AuditError("dependency lock is unreadable") from None
    packages = document.get("package")
    if not isinstance(packages, list) or len(packages) > MAX_PACKAGES:
        raise AuditError("dependency lock package inventory is invalid")
    resolved: set[tuple[str, str]] = set()
    for item in packages:
        if not isinstance(item, dict):
            raise AuditError("dependency lock package inventory is invalid")
        _validate_uv_artifacts(item)
        source = item.get("source")
        if not isinstance(source, dict) or "registry" not in source:
            continue
        raw_name, raw_version = item.get("name"), item.get("version")
        if not isinstance(raw_name, str) or not isinstance(raw_version, str):
            continue
        name = _normalise_package(raw_name)
        if not PACKAGE_RE.fullmatch(name) or not raw_version or len(raw_version) > 128:
            raise AuditError("dependency lock contains an invalid package identity")
        # uv may legitimately select different versions for disjoint platform
        # markers. Audit every selected pair instead of silently keeping one.
        resolved.add((name, raw_version))
    if not resolved:
        raise AuditError("dependency lock contains no auditable packages")
    return tuple(sorted(resolved))


def load_acceptances(root: pathlib.Path) -> dict[tuple[str, str], RiskAcceptance]:
    """Load exact, expiring advisory exceptions with mandatory justification.

    Format: ``ADVISORY-ID package expires=YYYY-MM-DD # justification``.
    Broad package-only suppressions are deliberately rejected.
    """

    path = root / ".security-audit-allow.txt"
    if not path.exists():
        return {}
    if path.is_symlink() or path.stat().st_size > 1024 * 1024:
        raise AuditError("security acceptance ledger is unavailable or too large")
    today = dt.date.today()
    accepted: dict[tuple[str, str], RiskAcceptance] = {}
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        declaration, separator, justification = stripped.partition("#")
        fields = declaration.split()
        if not separator or len(fields) != 3 or len(justification.strip()) < 12:
            raise AuditError(f"security acceptance line {number} is not justified")
        advisory_id, package, expiry_field = fields
        package = _normalise_package(package)
        expiry_match = EXPIRY_RE.fullmatch(expiry_field)
        if (
            not ADVISORY_RE.fullmatch(advisory_id)
            or not PACKAGE_RE.fullmatch(package)
            or expiry_match is None
        ):
            raise AuditError(f"security acceptance line {number} is invalid")
        try:
            expires = dt.date.fromisoformat(expiry_match.group(1))
        except ValueError:
            raise AuditError(f"security acceptance line {number} is invalid") from None
        if expires < today:
            raise AuditError(f"security acceptance line {number} has expired")
        if expires > today + dt.timedelta(days=MAX_ACCEPTANCE_DAYS):
            raise AuditError(
                f"security acceptance line {number} exceeds the review horizon"
            )
        key = (advisory_id.casefold(), package)
        if key in accepted:
            raise AuditError(f"security acceptance line {number} is duplicated")
        accepted[key] = RiskAcceptance(
            advisory_id=advisory_id,
            package=package,
            expires=expires,
            justification=justification.strip(),
        )
    return accepted


def _tls_context() -> ssl.SSLContext:
    cafile = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    capath = os.environ.get("SSL_CERT_DIR")
    try:
        return ssl.create_default_context(cafile=cafile or None, capath=capath or None)
    except (OSError, ssl.SSLError):
        raise AuditError("configured TLS trust material is invalid") from None


def _read_json(response: Any) -> dict[str, Any]:
    declared = response.headers.get("Content-Length")
    if declared:
        try:
            if int(declared) > MAX_RESPONSE_BYTES:
                raise AuditError("OSV response exceeds the safe bound")
        except ValueError:
            raise AuditError("OSV response length is invalid") from None
    raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise AuditError("OSV response exceeds the safe bound")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError):
        raise AuditError("OSV response is invalid") from None
    if not isinstance(value, dict):
        raise AuditError("OSV response is invalid")
    return value


def _request(url: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if url != OSV_BATCH:
        advisory_id = url.removeprefix(OSV_VULN_PREFIX)
        if not url.startswith(OSV_VULN_PREFIX) or not ADVISORY_RE.fullmatch(
            advisory_id
        ):
            raise AuditError("OSV request target is invalid")
    data = (
        None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    )
    headers = {"Accept": "application/json", "User-Agent": "agent-utilities-audit/1"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers)
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=_tls_context()),
        _RejectRedirects(),
    )
    try:
        with opener.open(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            if response.status != 200:
                raise AuditError("OSV service returned a non-success response")
            return _read_json(response)
    except AuditError:
        raise
    except (urllib.error.URLError, TimeoutError, OSError, ssl.SSLError):
        raise AuditError("OSV service is unavailable") from None


def _advisory_detail(advisory_id: str, package: str) -> tuple[str, ...]:
    detail = _request(OSV_VULN_PREFIX + advisory_id)
    fixed: set[str] = set()
    for affected in detail.get("affected") or []:
        if not isinstance(affected, dict):
            continue
        identity = affected.get("package") or {}
        if (
            not isinstance(identity, dict)
            or _normalise_package(str(identity.get("name") or "")) != package
        ):
            continue
        for version_range in affected.get("ranges") or []:
            if not isinstance(version_range, dict):
                continue
            for event in version_range.get("events") or []:
                if isinstance(event, dict) and isinstance(event.get("fixed"), str):
                    fixed.add(event["fixed"])
    return tuple(sorted(fixed))


def audit(
    packages: tuple[tuple[str, str], ...],
) -> list[tuple[str, str, str, tuple[str, ...]]]:
    selections = sorted(packages)
    findings: list[tuple[str, str, str, tuple[str, ...]]] = []
    for offset in range(0, len(selections), 100):
        chunk = selections[offset : offset + 100]
        response = _request(
            OSV_BATCH,
            payload={
                "queries": [
                    {
                        "package": {"name": name, "ecosystem": "PyPI"},
                        "version": version,
                    }
                    for name, version in chunk
                ]
            },
        )
        results = response.get("results")
        if not isinstance(results, list) or len(results) != len(chunk):
            raise AuditError("OSV batch response does not match the request")
        for (name, version), result in zip(chunk, results, strict=True):
            if not isinstance(result, dict):
                raise AuditError("OSV batch response is invalid")
            for vulnerability in result.get("vulns") or []:
                advisory_id = (
                    vulnerability.get("id") if isinstance(vulnerability, dict) else None
                )
                if not isinstance(advisory_id, str) or not ADVISORY_RE.fullmatch(
                    advisory_id
                ):
                    raise AuditError("OSV advisory identity is invalid")
                findings.append(
                    (
                        name,
                        version,
                        advisory_id,
                        _advisory_detail(advisory_id, name),
                    )
                )
    return findings


def _offline_warn_allowed() -> bool:
    return (
        os.environ.get("SECURITY_AUDIT_OFFLINE_POLICY", "").strip().casefold() == "warn"
    )


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) > 1:
        print("audit: expected at most one dependency-lock argument", file=sys.stderr)
        return 2
    lock = pathlib.Path(arguments[0] if arguments else "uv.lock")
    try:
        packages = parse_lock(lock)
        acceptances = load_acceptances(lock.resolve().parent)
        findings = audit(packages)
    except AuditError as error:
        if _offline_warn_allowed() and str(error) == "OSV service is unavailable":
            print(
                "audit: WARNING - OSV unavailable under explicit local offline policy"
            )
            return 0
        print(f"audit: FAILED - {error}", file=sys.stderr)
        return 2

    failures: list[tuple[str, str, str, tuple[str, ...]]] = []
    used_acceptances: set[tuple[str, str]] = set()
    for name, version, advisory_id, fixed in sorted(findings):
        key = (advisory_id.casefold(), name)
        if key in acceptances:
            used_acceptances.add(key)
            print(
                f"  ACCEPTED {name} {version} {advisory_id} "
                f"until {acceptances[key].expires.isoformat()}"
            )
            continue
        failures.append((name, version, advisory_id, fixed))
        remediation = ",".join(fixed) if fixed else "no-fixed-release"
        print(f"  FAIL {name} {version} {advisory_id} fixed={remediation}")

    unused = sorted(set(acceptances) - used_acceptances)
    for advisory_id, package in unused:
        print(f"  FAIL stale acceptance {package} {advisory_id}")
    if failures or unused:
        print(
            "audit: dependency vulnerabilities or stale risk acceptances require "
            "review",
            file=sys.stderr,
        )
        return 1
    print(f"audit: clean ({len(packages)} pinned packages, {len(findings)} findings)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
