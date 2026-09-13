"""Focused executable contract tests for the Rust architecture gate."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "tests/fixtures/arch_lint_gate"
pytestmark = pytest.mark.no_engine


def _module():
    scripts = str(REPO / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    path = REPO / "scripts/check_rust_arch_lint.py"
    spec = importlib.util.spec_from_file_location("eg_check_rust_arch_lint", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _finding(source: str, needle: str, message: str) -> dict:
    offset = source.index(needle)
    line = source.count("\n", 0, offset) + 1
    line_start = source.rfind("\n", 0, offset) + 1
    return {
        "code": "AL002",
        "rule": "no-sync-io",
        "severity": "error",
        "location": {
            "file": "src/lib.rs",
            "line": line,
            "column": offset - line_start + 1,
            "offset": 0,
            "length": 0,
        },
        "message": message,
        "suggestion": None,
        "labels": [],
    }


def _classifications(module, source: str, findings: list[dict]):
    report = {"files_checked": 1, "violations": findings}
    return module.classify_report(report, {"src/lib.rs": source})


def test_policy_names_every_supported_rule_and_has_no_broad_source_exclusion():
    module = _module()
    policy = module.load_policy()
    assert "preset" not in policy
    assert policy["analyzer"]["exclude"] == ["**/target/**", "**/target-*/**"]
    assert set(policy["rules"]) == set(module.RULES)
    assert policy["rules"]["no-unwrap-expect"] == {
        "enabled": False,
        "severity": "error",
    }
    assert all(
        policy["rules"][name]
        == {
            "enabled": True,
            "severity": module.RULES[name][2],
        }
        for name in module.RULES
        if name != "no-unwrap-expect"
    )


def test_sync_async_spawn_test_and_unresolved_receiver_are_distinct():
    module = _module()
    source = """
fn startup() { let _ = std::fs::read("a"); }
async fn served() { let _ = std::fs::read("b"); }
async fn offloaded() {
    let _ = ::tokio::task::spawn_blocking(|| { std::fs::read("c") }).await;
}
#[cfg(test)]
mod tests { #[tokio::test] async fn case() { let _ = std::fs::read("d"); } }
struct VirtualPath;
async fn unknown(value: VirtualPath) { let _ = value.exists(); }
""".lstrip()
    findings = [
        _finding(
            source, 'std::fs::read("a")', "Synchronous I/O `std::fs::read` may block"
        ),
        _finding(
            source, 'std::fs::read("b")', "Synchronous I/O `std::fs::read` may block"
        ),
        _finding(
            source, 'std::fs::read("c")', "Synchronous I/O `std::fs::read` may block"
        ),
        _finding(
            source, 'std::fs::read("d")', "Synchronous I/O `std::fs::read` may block"
        ),
        _finding(source, "value.exists()", "Synchronous I/O `.exists()` may block"),
    ]
    result = _classifications(module, source, findings)
    assert [item["classification"] for item in result["al002_dispositions"]] == [
        "sync_context",
        "async_direct",
        "spawn_blocking_context",
        "test_context",
        "receiver_unresolved",
    ]
    assert len(result["blocking_violations"]) == 1
    assert result["raw_report"] == {"files_checked": 1, "violations": findings}


def test_unresolved_custom_spawn_blocking_name_does_not_create_offload_context():
    module = _module()
    source = """
fn spawn_blocking<F, T>(work: F) -> T where F: FnOnce() -> T { work() }
async fn served() {
    let _ = spawn_blocking(|| { std::fs::read("still-on-executor") });
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("still-on-executor")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_scoped_tokio_spawn_blocking_import_is_resolved():
    module = _module()
    source = """
async fn served() {
    let _ = offload(|| { std::fs::read("off-executor") });
    use ::tokio::task::spawn_blocking as offload;
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("off-executor")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "spawn_blocking_context"


def test_absolute_tokio_spawn_blocking_expression_is_resolved():
    module = _module()
    source = """
async fn served() {
    let _ = ::tokio::task::spawn_blocking(|| std::fs::read("off-executor")).await;
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("off-executor")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "spawn_blocking_context"


def test_shadowable_tokio_path_does_not_create_offload_context():
    module = _module()
    source = """
mod tokio { pub mod task { pub fn spawn_blocking<F, T>(work: F) -> T where F: FnOnce()
-> T { work() } } }
async fn served() {
    let _ = tokio::task::spawn_blocking(|| std::fs::read("still-on-executor")).await;
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("still-on-executor")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_shadowable_tokio_import_does_not_create_offload_context():
    module = _module()
    source = """
use tokio::task::spawn_blocking as offload;
async fn served() {
    let _ = offload(|| std::fs::read("still-on-executor")).await;
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("still-on-executor")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_shadowed_tokio_import_does_not_create_offload_context():
    module = _module()
    source = """
use ::tokio::task::spawn_blocking;
async fn served() {
    let spawn_blocking = |work: fn() -> Vec<u8>| work();
    let _ = spawn_blocking(|| { std::fs::read("still-on-executor").unwrap() });
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("still-on-executor")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_async_unsafe_functions_and_async_closures_are_async_contexts():
    module = _module()
    source = """
async unsafe fn unsafe_async() { let _ = std::fs::read("one"); }
pub async unsafe fn public_unsafe_async() { let _ = std::fs::read("two"); }
fn closures() {
    let _without_move = async || { std::fs::read("three") };
    let _with_move = async move || std::fs::read("four");
}
""".lstrip()
    findings = [
        _finding(
            source,
            f'std::fs::read("{number}")',
            "Synchronous I/O `std::fs::read` may block",
        )
        for number in ("one", "two", "three", "four")
    ]
    result = _classifications(module, source, findings)
    assert [item["classification"] for item in result["al002_dispositions"]] == [
        "async_direct"
    ] * 4
    assert len(result["blocking_violations"]) == 4


def test_async_signature_const_braces_do_not_replace_the_function_body():
    module = _module()
    source = """
async fn served(_: [u8; { 1 + 1 }]) -> Result<[u8; { 2 + 2 }], Error>
where
    Error: From<Problem<{ 3 + 3 }>>,
{
    let _ = std::fs::read("inside-real-body");
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("inside-real-body")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_innermost_execution_context_wins():
    module = _module()
    source = """
fn startup() {
    run(async { let _ = std::fs::read("async-block"); });
}
async fn outer() {
    fn offline() { let _ = std::fs::read("nested-sync"); }
    offline();
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("async-block")',
            "Synchronous I/O `std::fs::read` may block",
        ),
        _finding(
            source,
            'std::fs::read("nested-sync")',
            "Synchronous I/O `std::fs::read` may block",
        ),
    ]
    result = _classifications(module, source, findings)
    assert [item["classification"] for item in result["al002_dispositions"]] == [
        "async_direct",
        "sync_context",
    ]


def test_comment_string_raw_string_and_character_braces_do_not_change_context():
    module = _module()
    source = r"""
async fn served() {
    let _text = "}";
    let _raw = r#"}"#;
    let _character = '}';
    /* } */
    let _ = std::fs::read("still-async");
}
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("still-async")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_raw_string_hash_count_is_unbounded_when_masking_braces():
    module = _module()
    hashes = "#" * 17
    source = (
        "async fn served() {\n"
        f'    let _raw = r{hashes}"}}"{hashes};\n'
        '    let _ = std::fs::read("still-async");\n'
        "}\n"
    )
    findings = [
        _finding(
            source,
            'std::fs::read("still-async")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_cfg_not_test_does_not_hide_async_io():
    module = _module()
    source = """
#[cfg(not(test))]
async fn served() { let _ = std::fs::read("production"); }
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("production")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_cfg_any_test_or_feature_remains_production_reachable():
    module = _module()
    source = """
#[cfg(any(test, feature = "harness"))]
async fn served() { let _ = std::fs::read("production-feature"); }
""".lstrip()
    findings = [
        _finding(
            source,
            'std::fs::read("production-feature")',
            "Synchronous I/O `std::fs::read` may block",
        )
    ]
    result = _classifications(module, source, findings)
    assert result["al002_dispositions"][0]["classification"] == "async_direct"


def test_aliases_are_found_without_rewriting_raw_scanner_findings():
    module = _module()
    source = 'use std::fs as disk;\nasync fn served() { let _ = disk::read("x"); }\n'
    result = _classifications(module, source, [])
    assert result["raw_summary"]["violations"] == 0
    assert [
        (item["operation"], item["classification"]) for item in result["alias_findings"]
    ] == [("std::fs::read", "async_alias")]
    assert len(result["blocking_violations"]) == 1


def test_file_type_alias_is_detected():
    module = _module()
    source = """
use std::fs::File as DiskFile;
async fn served() { let _ = DiskFile::open("snapshot.redb"); }
""".lstrip()
    result = _classifications(module, source, [])
    assert [item["operation"] for item in result["alias_findings"]] == [
        "std::fs::File::open"
    ]
    assert len(result["blocking_violations"]) == 1


def test_grouped_self_and_file_imports_cover_their_legal_block_scope():
    module = _module()
    source = """
async fn served() {
    let _ = fs::read("before-import");
    let _ = File::open("also-before-import");
    use std::fs::{self, File};
}
""".lstrip()
    result = _classifications(module, source, [])
    assert [
        (item["operation"], item["classification"]) for item in result["alias_findings"]
    ] == [
        ("std::fs::read", "async_alias"),
        ("std::fs::File::open", "async_alias"),
    ]
    assert len(result["blocking_violations"]) == 2


def test_nested_grouped_aliases_cover_their_legal_block_scope():
    module = _module()
    source = """
async fn served() {
    let _ = disk::read("before-import");
    let _ = DiskFile::open("also-before-import");
    use std::{fs::{self as disk, File as DiskFile}, path::Path};
}
""".lstrip()
    result = _classifications(module, source, [])
    assert [
        (item["operation"], item["classification"]) for item in result["alias_findings"]
    ] == [
        ("std::fs::read", "async_alias"),
        ("std::fs::File::open", "async_alias"),
    ]
    assert len(result["blocking_violations"]) == 2


def test_flat_path_inside_std_group_is_resolved():
    module = _module()
    source = """
use ::std::{fs::read as load, path::Path};
async fn served() { let _ = load("snapshot.redb"); }
""".lstrip()
    result = _classifications(module, source, [])
    assert [item["operation"] for item in result["alias_findings"]] == ["std::fs::read"]
    assert len(result["blocking_violations"]) == 1


def test_unsupported_nested_std_fs_import_fails_closed():
    module = _module()
    source = "use std::{fs::{nested::{read}}};\n"
    with pytest.raises(module.GateError, match="cannot be proven safe"):
        _classifications(module, source, [])


def test_std_fs_glob_import_fails_closed():
    module = _module()
    with pytest.raises(module.GateError, match="glob import cannot be proven safe"):
        _classifications(module, "use std::fs::*;\n", [])


def test_alias_scope_does_not_leak_out_of_its_block():
    module = _module()
    source = """
async fn fixture() {
    use std::{fs as disk, path::Path};
    let _ = disk::read("test");
}
async fn unrelated() { let _ = disk::read("not-an-import"); }
""".lstrip()
    result = _classifications(module, source, [])
    assert len(result["alias_findings"]) == 1
    assert result["alias_findings"][0]["classification"] == "async_alias"


def test_rule_severity_is_bound_to_the_configured_rule_identity():
    module = _module()
    finding = _finding("fn f() {}", "fn", "future rule error")
    finding.update(code="AL003", rule="no-error-swallowing")
    with pytest.raises(module.GateError, match="severity does not match"):
        module.classify_report(
            {"files_checked": 1, "violations": [finding]},
            {"src/lib.rs": "fn f() {}"},
        )


def test_rule_name_is_bound_to_the_configured_code():
    module = _module()
    finding = _finding("fn f() {}", "fn", "wrong identity")
    finding.update(rule="handler-complexity")
    with pytest.raises(module.GateError, match="rule identity does not match"):
        module.classify_report(
            {"files_checked": 1, "violations": [finding]},
            {"src/lib.rs": "fn f() {}"},
        )


def test_report_file_count_and_path_must_match_source_universe():
    module = _module()
    with pytest.raises(module.GateError, match="does not match"):
        module.classify_report(
            {"files_checked": 0, "violations": []}, {"src/lib.rs": ""}
        )
    outside = _finding("fn f() {}", "fn", "Synchronous I/O `std::fs::read` may block")
    outside["location"]["file"] = "outside.rs"
    with pytest.raises(module.GateError, match="outside the universe"):
        module.classify_report(
            {"files_checked": 1, "violations": [outside]}, {"src/lib.rs": "fn f() {}"}
        )
    disabled = _finding("fn f() {}", "fn", "disabled rule")
    disabled.update(code="AL001", rule="no-unwrap-expect")
    with pytest.raises(module.GateError, match="outside the enabled policy"):
        module.classify_report(
            {"files_checked": 1, "violations": [disabled]},
            {"src/lib.rs": "fn f() {}"},
        )


@pytest.mark.parametrize(
    ("fixture", "expected_blockers"),
    [
        ("known_good.rs.fixture", 0),
        ("known_bad.rs.fixture", 2),
        ("custom_spawn_name.rs.fixture", 1),
        ("cfg_production_reachable.rs.fixture", 1),
        ("scoped_alias.rs.fixture", 2),
        ("async_syntax.rs.fixture", 4),
        ("nested_grouped_alias.rs.fixture", 2),
        ("shadowable_tokio_path.rs.fixture", 1),
        ("const_signature.rs.fixture", 1),
        ("long_raw_string.rs.fixture", 1),
    ],
)
def test_installed_scanner_fixture_contract(
    tmp_path: Path, fixture: str, expected_blockers: int
):
    """Exercise the real pinned static scanner on tiny known-good/bad inputs."""

    module = _module()
    source = (FIXTURES / fixture).read_text(encoding="utf-8")
    rust_file = tmp_path / "lib.rs"
    rust_file.write_text(source, encoding="utf-8")
    binary = module.resolve_binary("arch-lint", "ARCH_LINT_BIN")
    result = subprocess.run(
        [binary, "check", "--format", "json", "--rules", "AL002", str(tmp_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode in {0, 1}, result.stderr
    report = module.parse_scanner_json(result.stdout)
    classified = module.classify_report(report, {"lib.rs": source})
    assert len(classified["blocking_violations"]) == expected_blockers
