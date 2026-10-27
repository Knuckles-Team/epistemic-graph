"""The exact upstream-source boundary never becomes a directory allowlist."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine
ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _minimal_policy(
    path: str,
    digest: str,
    *,
    finding_line: int | None = None,
    finding_kind: str = "synthetic_aws_example",
    content_sha256: str | None = None,
) -> str:
    finding = "finding = []\n\n"
    if finding_line is not None:
        assert content_sha256 is not None
        finding = (
            "[[finding]]\n"
            f'path = "{path}"\nline = {finding_line}\n'
            f'kind = "{finding_kind}"\n'
            'reason = "reviewed upstream fixture"\n'
            f'sha256 = "{digest}"\n'
            f'content_sha256 = "{content_sha256}"\n\n'
        )
    return (
        finding + '[policy]\nversion = 1\nscope = "exact-file-sha256-only"\n\n'
        "[[package]]\n"
        'name = "fixture-upstream"\nversion = "1.0.0"\n'
        'license = "Apache-2.0"\nattribution = "Upstream contributors"\n'
        f'archive_sha256 = "{"a" * 64}"\n'
        'provenance = "immutable fixture archive"\n\n'
        "[[source]]\n"
        f'path = "{path}"\nsha256 = "{digest}"\n'
        'package = "fixture-upstream"\n'
        'reason = "exact upstream fixture bytes"\n'
    )


def test_committed_policy_has_exact_review_receipts() -> None:
    policy = _load("third_party_sources")
    verified, errors, findings = policy.verify(ROOT)

    assert errors == []
    assert len(verified) == 892
    assert policy.package_count(ROOT) == 8
    assert sum(item.kind == "synthetic_aws_example" for item in findings) == 3
    assert sum(item.kind == "binary_fixture" for item in findings) == 6
    assert all(item.path in verified for item in findings)


def test_hash_drift_is_fail_closed(tmp_path: Path) -> None:
    policy = _load("third_party_sources")
    source = tmp_path / "vendor" / "upstream" / "fixture.txt"
    source.parent.mkdir(parents=True)
    original = b"licensed upstream bytes\n"
    source.write_bytes(original)
    ledger = tmp_path / ".config"
    ledger.mkdir()
    (ledger / "third-party-sources.toml").write_text(
        _minimal_policy("vendor/upstream/fixture.txt", _sha256(original)),
        encoding="utf-8",
    )

    source.write_bytes(b"changed bytes\n")
    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert any("does not match its pinned sha256" in error for error in errors)


def test_security_scans_first_party_patch_and_unlisted_vendor(tmp_path: Path) -> None:
    sanitizer = _load("security_sanitizer")
    reviewed = b"AWS_ACCESS_KEY_ID = '" + b"AKIA" + b"ABCDEFGHIJKLMNOP" + b"'\n"
    unreviewed = b"AWS_SECRET_ACCESS_KEY = '" + b"ASIA" + b"ABCDEFGHIJKLMNOP" + b"'\n"
    fixture = reviewed + unreviewed
    listed = tmp_path / "vendor" / "upstream" / "fixture.rs"
    listed.parent.mkdir(parents=True)
    listed.write_bytes(fixture)
    config = tmp_path / ".config"
    config.mkdir()
    (config / "third-party-sources.toml").write_text(
        _minimal_policy(
            "vendor/upstream/fixture.rs",
            _sha256(fixture),
            finding_line=1,
            content_sha256=_sha256(reviewed.rstrip(b"\n")),
        ),
        encoding="utf-8",
    )
    patch = tmp_path / ".config" / "security-patch.toml"
    patch.write_bytes(fixture)
    unlisted = tmp_path / "vendor" / "unreviewed.rs"
    unlisted.write_bytes(fixture)

    findings = sanitizer.scan_repository(tmp_path)
    assert any(".config/security-patch.toml" in finding for finding in findings)
    assert any("vendor/unreviewed.rs" in finding for finding in findings)
    assert any("vendor/upstream/fixture.rs:2" in finding for finding in findings)
    assert not any("vendor/upstream/fixture.rs:1" in finding for finding in findings)


def test_reviewed_finding_line_is_exact(tmp_path: Path) -> None:
    sanitizer = _load("security_sanitizer")
    reviewed = b"AWS_ACCESS_KEY_ID = '" + b"AKIA" + b"ABCDEFGHIJKLMNOP" + b"'\n"
    clean = b"ordinary source\n"
    source = tmp_path / "vendor" / "upstream" / "fixture.rs"
    source.parent.mkdir(parents=True)
    source.write_bytes(reviewed + clean)
    config = tmp_path / ".config"
    config.mkdir()
    (config / "third-party-sources.toml").write_text(
        _minimal_policy(
            "vendor/upstream/fixture.rs",
            _sha256(source.read_bytes()),
            finding_line=2,
            content_sha256=_sha256(clean.rstrip(b"\n")),
        ),
        encoding="utf-8",
    )

    findings = sanitizer.scan_repository(tmp_path)
    assert any("vendor/upstream/fixture.rs:1" in finding for finding in findings)


def test_reviewed_finding_hash_drift_is_fail_closed(tmp_path: Path) -> None:
    policy = _load("third_party_sources")
    source = tmp_path / "vendor" / "upstream" / "fixture.rs"
    source.parent.mkdir(parents=True)
    source.write_bytes(
        b"AWS_ACCESS_KEY_ID = '" + b"AKIA" + b"ABCDEFGHIJKLMNOP" + b"'\n"
    )
    config = tmp_path / ".config"
    config.mkdir()
    (config / "third-party-sources.toml").write_text(
        _minimal_policy(
            "vendor/upstream/fixture.rs",
            _sha256(source.read_bytes()),
            finding_line=1,
            content_sha256="a" * 64,
        ),
        encoding="utf-8",
    )

    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert any("content_sha256" in error for error in errors)


def test_only_exact_reviewed_binary_fixture_is_exempt(tmp_path: Path) -> None:
    sanitizer = _load("security_sanitizer")
    reviewed_bytes = b"\x00\xfflicensed-upstream-fixture"
    reviewed = tmp_path / "vendor" / "upstream" / "reviewed.avro"
    reviewed.parent.mkdir(parents=True)
    reviewed.write_bytes(reviewed_bytes)
    (reviewed.parent / "unreviewed.avro").write_bytes(reviewed_bytes)
    digest = _sha256(reviewed_bytes)
    config = tmp_path / ".config"
    config.mkdir()
    (config / "third-party-sources.toml").write_text(
        _minimal_policy(
            "vendor/upstream/reviewed.avro",
            digest,
            finding_line=1,
            finding_kind="binary_fixture",
            content_sha256=digest,
        ),
        encoding="utf-8",
    )

    findings = sanitizer.scan_repository(tmp_path)
    assert not any("vendor/upstream/reviewed.avro" in finding for finding in findings)
    assert any("vendor/upstream/unreviewed.avro" in finding for finding in findings)


def test_privacy_scans_first_party_metadata_but_preserves_upstream_attribution(
    tmp_path: Path,
) -> None:
    privacy = _load("check_tracked_privacy")
    upstream = (
        b"[package]\n"
        b'name = "fixture-upstream"\n'
        b'authors = ["Upstream contributors"]\n'
        b'description = "Fixture from /'
        b'home/upstream/warehouse"\n'
    )
    vendor_path = tmp_path / "vendor" / "upstream" / "Cargo.toml"
    vendor_path.parent.mkdir(parents=True)
    vendor_path.write_bytes(upstream)
    config = tmp_path / ".config"
    config.mkdir()
    source_hash = _sha256(upstream)
    author_hash = _sha256(upstream.splitlines()[2])
    path_hash = _sha256(upstream.splitlines()[3])
    policy_text = _minimal_policy(
        "vendor/upstream/Cargo.toml",
        source_hash,
        finding_line=3,
        finding_kind="upstream_privacy_example",
        content_sha256=author_hash,
    )
    policy_text += (
        "[[finding]]\n"
        'path = "vendor/upstream/Cargo.toml"\nline = 4\n'
        'kind = "upstream_privacy_example"\n'
        'reason = "upstream attribution and documented fixture path"\n'
        f'sha256 = "{source_hash}"\ncontent_sha256 = "{path_hash}"\n\n'
    )
    (config / "third-party-sources.toml").write_text(
        policy_text,
        encoding="utf-8",
    )
    first_party = tmp_path / "Cargo.toml"
    first_party.write_bytes(upstream)

    findings = privacy.scan(tmp_path)
    assert any(item.path == "Cargo.toml" for item in findings)
    assert not any(item.path == "vendor/upstream/Cargo.toml" for item in findings)


def test_malformed_missing_and_traversal_ledgers_fail_closed(tmp_path: Path) -> None:
    policy = _load("third_party_sources")
    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert errors == []

    config = tmp_path / ".config"
    config.mkdir()
    ledger = config / "third-party-sources.toml"
    ledger.write_text(
        '[policy]\nversion = 1\nscope = "exact-file-sha256-only"\n',
        encoding="utf-8",
    )
    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert errors

    source = tmp_path / "outside.rs"
    source.write_text("source", encoding="utf-8")
    ledger.write_text(
        _minimal_policy("vendor/../outside.rs", _sha256(source.read_bytes())),
        encoding="utf-8",
    )
    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert any("vendor/third_party" in error for error in errors)


def test_parent_and_final_symlink_escapes_fail_closed(tmp_path: Path) -> None:
    policy = _load("third_party_sources")
    outside = tmp_path / "outside"
    outside.mkdir()
    external = outside / "fixture.rs"
    external.write_text("upstream", encoding="utf-8")
    config = tmp_path / ".config"
    config.mkdir()

    parent_root = tmp_path / "vendor" / "parent-root"
    parent_root.mkdir(parents=True)
    (parent_root / "upstream").symlink_to(outside, target_is_directory=True)
    (config / "third-party-sources.toml").write_text(
        _minimal_policy(
            "vendor/parent-root/upstream/fixture.rs",
            _sha256(external.read_bytes()),
        ),
        encoding="utf-8",
    )
    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert any("symlinked parent" in error for error in errors)

    final_root = tmp_path / "vendor" / "final-root"
    final_root.mkdir(parents=True)
    (final_root / "fixture.rs").symlink_to(external)
    (config / "third-party-sources.toml").write_text(
        _minimal_policy(
            "vendor/final-root/fixture.rs",
            _sha256(external.read_bytes()),
        ),
        encoding="utf-8",
    )
    verified, errors, _ = policy.verify(tmp_path)
    assert verified == set()
    assert any("is a symlink" in error for error in errors)
