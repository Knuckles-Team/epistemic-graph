"""EG-CONTRACT-R018: the crates gate follows the declared pyo3 feature surface.

Collected by the existing python-suite job. These checks parse configuration;
they do not replace the native crates gate's compilation and test evidence.
"""

import shlex
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ("eg-numeric", "eg-pyengine")


@pytest.fixture
def release_matrix():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    manifests = {
        package: tomllib.loads((ROOT / "crates" / package / "Cargo.toml").read_text())
        for package in PACKAGES
    }
    return workflow, manifests


def _crates_step(workflow):
    # Select executable cargo-test arguments, not a step label or a comment
    # elsewhere in the workflow. Fail if the job loses or duplicates this gate.
    matches = []
    for step in workflow["jobs"]["gates-pyo3"]["steps"]:
        tokens = shlex.split(step.get("run", ""), comments=True)
        if "cargo" in tokens and tokens[tokens.index("cargo") + 1 :][:1] == ["test"]:
            packages = {
                tokens[i + 1] for i, token in enumerate(tokens) if token == "-p"
            }
            if packages == set(PACKAGES):
                matches.append(step)
    assert len(matches) == 1, "expected one pyo3 crates test command"
    return matches[0]


def _enabled_features(selected, manifests):
    enabled = set()
    pending = list(selected)
    while pending:
        qualified = pending.pop()
        if qualified in enabled:
            continue
        enabled.add(qualified)
        package, feature = qualified.split("/", 1)
        features = manifests[package]["features"]
        pending.extend(
            f"{package}/{child}"
            for child in features.get(feature, [])
            if child in features
        )
    return enabled


def _assert_matrix(workflow, manifests):
    tokens = shlex.split(_crates_step(workflow)["run"], comments=True)
    assert tokens.count("--features") == 1, "expected explicit crates feature matrix"
    selected = set(tokens[tokens.index("--features") + 1].split(","))
    # The crates job promises every feature except python: extension-module
    # cannot link test binaries. `default` is Cargo's aggregate, not a domain.
    expected = {
        f"{package}/{feature}"
        for package, manifest in manifests.items()
        for feature in manifest["features"]
        if feature not in {"default", "python"}
    }
    assert "eg-numeric/motif" in expected, "motif must remain a declared feature"
    assert selected <= expected, (
        f"release feature drift: unexpected={sorted(selected - expected)}"
    )
    actual = _enabled_features(selected, manifests)
    assert actual == expected, (
        f"release feature drift: missing={sorted(expected - actual)}, "
        f"unexpected={sorted(actual - expected)}"
    )


@pytest.mark.spec("EG-CONTRACT-R018")
def test_release_crates_match_manifest_features(release_matrix):
    _assert_matrix(*release_matrix)


def test_existing_python_suite_collects_root_tests(release_matrix):
    workflow, _ = release_matrix
    commands = [
        shlex.split(step.get("run", "").replace("\\\n", ""), comments=True)
        for step in workflow["jobs"]["python-suite"]["steps"]
    ]
    assert any(
        tokens[i : i + 4] == ["python3", "-m", "pytest", "tests/"]
        for tokens in commands
        for i in range(len(tokens))
    ), "existing Python suite must collect the root tests, including this check"


@pytest.mark.parametrize("package", PACKAGES)
def test_each_declared_feature_is_required(release_matrix, package):
    workflow, manifests = release_matrix
    step = _crates_step(workflow)
    original = step["run"]
    tokens = shlex.split(original)
    features = {
        feature
        for feature in tokens[tokens.index("--features") + 1].split(",")
        if feature.startswith(f"{package}/")
    }
    assert features
    for qualified in sorted(features):
        # Mutate the real command, including motif; a comment cannot restore it.
        step["run"] = (
            original.replace(qualified + ",", "").replace("," + qualified + " ", " ")
            + f" # {qualified}"
        )
        with pytest.raises(AssertionError, match="release feature drift"):
            _assert_matrix(workflow, manifests)


@pytest.mark.parametrize("package", PACKAGES)
def test_new_manifest_feature_requires_release_coverage(release_matrix, package):
    workflow, manifests = release_matrix
    manifests[package]["features"]["new-test-feature"] = []
    with pytest.raises(AssertionError, match=f"{package}/new-test-feature"):
        _assert_matrix(workflow, manifests)
    step = _crates_step(workflow)
    step["run"] = step["run"].replace(
        "--features ", f"--features {package}/new-test-feature,"
    )
    _assert_matrix(workflow, manifests)


@pytest.mark.parametrize("feature", ["python", "unknown-feature"])
def test_unexpected_release_features_fail(release_matrix, feature):
    workflow, manifests = release_matrix
    step = _crates_step(workflow)
    step["run"] = step["run"].replace(
        "--features ", f"--features eg-numeric/{feature},"
    )
    with pytest.raises(AssertionError, match="release feature drift"):
        _assert_matrix(workflow, manifests)


def test_removing_motif_from_both_surfaces_still_fails(release_matrix):
    workflow, manifests = release_matrix
    del manifests["eg-numeric"]["features"]["motif"]
    step = _crates_step(workflow)
    step["run"] = step["run"].replace("eg-numeric/motif,", "")
    with pytest.raises(AssertionError, match="motif must remain"):
        _assert_matrix(workflow, manifests)


def test_transitive_feature_drift_fails(release_matrix):
    workflow, manifests = release_matrix
    manifests["eg-pyengine"]["features"]["ml-pipeline"].remove("graphlearn")
    with pytest.raises(AssertionError, match="eg-pyengine/graphlearn"):
        _assert_matrix(workflow, manifests)


@pytest.mark.parametrize("mutation", ["remove", "duplicate", "comment", "build"])
def test_crates_gate_cannot_disappear(release_matrix, mutation):
    workflow, manifests = release_matrix
    step = _crates_step(workflow)
    steps = workflow["jobs"]["gates-pyo3"]["steps"]
    if mutation == "remove":
        steps.remove(step)
    elif mutation == "duplicate":
        steps.append(step.copy())
    elif mutation == "comment":
        step["run"] = "# " + step["run"]
    else:
        step["run"] = step["run"].replace("cargo test", "cargo build")
    with pytest.raises(AssertionError, match="expected one pyo3 crates test command"):
        _assert_matrix(workflow, manifests)


def _assert_required_clippy_declarations(workflow):
    job = workflow["jobs"]["quality-advisory"]
    profiles = job["strategy"]["matrix"]["profile"]
    required = {
        "published-full": "--no-default-features --features full,ast-extended",
        "workspace-all-features": "--workspace --all-features",
    }
    for name, flags in required.items():
        matches = [profile for profile in profiles if profile["name"] == name]
        assert len(matches) == 1, f"required Clippy profile must occur once: {name}"
        assert shlex.split(matches[0]["flags"]) == shlex.split(flags)
    commands = [
        step for step in job["steps"] if step.get("run", "").startswith("cargo clippy ")
    ]
    assert len(commands) == 1, "expected one executable Clippy command"
    command = commands[0]
    assert "if" not in command and not command.get("continue-on-error", False)
    assert not job.get("continue-on-error", False)
    assert command["run"] == (
        "cargo clippy --locked --keep-going ${{ matrix.profile.flags }} "
        "--all-targets -- -D warnings"
    )
    headers = [
        step
        for step in job["steps"]
        if step.get("name") == "Install librdkafka/Cyrus-SASL build headers"
    ]
    assert len(headers) == 1 and "if" not in headers[0]
    assert "libsasl2-dev" in shlex.split(headers[0]["run"], comments=True)
    assert "libcurl4-openssl-dev" in shlex.split(headers[0]["run"], comments=True)


# This is partial parent declaration coverage, not native execution evidence.
@pytest.mark.spec("EG-DURABLE-KERNEL-R011")
@pytest.mark.spec("EG-DURABLE-KERNEL-R011.5")
@pytest.mark.spec("EG-CONTRACT-R037")
def test_required_clippy_profiles_and_command_are_declared(release_matrix):
    workflow, _ = release_matrix
    _assert_required_clippy_declarations(workflow)


def _drop_published_full_profile(job):
    profiles = job["strategy"]["matrix"]["profile"]
    profiles[:] = [p for p in profiles if p["name"] != "published-full"]


def _break_profile_flags(job):
    job["strategy"]["matrix"]["profile"][0]["flags"] = "--workspace"


def _comment_out_clippy_command(job):
    command = next(
        s for s in job["steps"] if s.get("run", "").startswith("cargo clippy ")
    )
    command["run"] = "# " + command["run"]


def _drop_header_install_step(job):
    job["steps"] = [
        s
        for s in job["steps"]
        if s.get("name") != "Install librdkafka/Cyrus-SASL build headers"
    ]


CLIPPY_DECLARATION_MUTATIONS = {
    "profile": _drop_published_full_profile,
    "flags": _break_profile_flags,
    "command": _comment_out_clippy_command,
    "headers": _drop_header_install_step,
}


@pytest.mark.spec("EG-DURABLE-KERNEL-R011.5")
@pytest.mark.parametrize("mutation", list(CLIPPY_DECLARATION_MUTATIONS))
def test_required_clippy_declaration_drift_is_refused(release_matrix, mutation):
    workflow, _ = release_matrix
    CLIPPY_DECLARATION_MUTATIONS[mutation](workflow["jobs"]["quality-advisory"])
    with pytest.raises(AssertionError):
        _assert_required_clippy_declarations(workflow)


# spec: EG-CONTRACT-R018
def test_pyo3_gate_is_independent_required_and_keeps_trusted_cache_policy(
    release_matrix,
):
    workflow, _ = release_matrix
    jobs = workflow["jobs"]
    job = jobs["gates-pyo3"]
    assert job["needs"] == "changes"
    assert job["if"] == "needs.changes.outputs.code == 'true'"
    assert (
        job["timeout-minutes"]
        == "${{ github.event_name == 'pull_request' && 45 || 60 }}"
    )
    assert job["runs-on"] == "ubuntu-latest"
    assert job["env"] == jobs["gates-crates"]["env"]
    assert (
        job["env"]["SCCACHE_GHA_RW_MODE"]
        == "${{ github.ref == 'refs/heads/main' && 'READ_WRITE' || 'READ_ONLY' }}"
    )
    caches = [
        step
        for step in job["steps"]
        if step.get("uses", "").startswith("Swatinem/rust-cache@")
    ]
    assert len(caches) == 1
    assert caches[0]["with"]["save-if"] == "${{ github.ref == 'refs/heads/main' }}"
    steps = {step.get("name"): step for step in job["steps"]}
    assert steps["Test (pyo3 crates, every feature except python)"]["if"] == (
        "github.job == 'gates-pyo3'"
    )
    assert steps["Test (storage crate, isolated crash-process lifecycle)"]["if"] == (
        "github.job == 'gates-storage'"
    )
    assert job["steps"] == jobs["gates-crates"]["steps"]
    for consumer in ("engine-main-wheel",):
        assert "gates-pyo3" in jobs[consumer]["needs"]


# Partial declaration coverage, not native execution evidence: the slim
# profile's compile-and-lint evidence remains owned by the CI legs.
@pytest.mark.spec("EG-CONTRACT-R046")
def test_release_workflow_checks_the_slim_server_profile(release_matrix):
    workflow, _ = release_matrix
    profiles = workflow["jobs"]["quality-advisory"]["strategy"]["matrix"]["profile"]
    matches = [profile for profile in profiles if profile["name"] == "slim-server"]
    assert len(matches) == 1, "slim-server Clippy profile must occur once"
    assert shlex.split(matches[0]["flags"]) == shlex.split(
        "--no-default-features --features server"
    )
    runs = [
        str(step.get("run", "")) for step in workflow["jobs"]["gates-variants"]["steps"]
    ]
    assert any("--no-default-features --features server" in run for run in runs), (
        "no gate compiles the slim server profile"
    )


# Declaration coverage only: the slim-server Clippy matrix leg lints
# `--no-default-features --features server` with warnings denied (not merely
# reported) by the single blocking Clippy command. The native execution
# evidence remains owned by the CI leg.
@pytest.mark.spec("EG-CONTRACT-R032")
def test_slim_server_clippy_profile_lints_with_warnings_denied(release_matrix):
    workflow, _ = release_matrix
    job = workflow["jobs"]["quality-advisory"]
    profiles = job["strategy"]["matrix"]["profile"]
    matches = [profile for profile in profiles if profile["name"] == "slim-server"]
    assert len(matches) == 1, "slim-server Clippy profile must occur once"
    assert shlex.split(matches[0]["flags"]) == shlex.split(
        "--no-default-features --features server"
    )
    commands = [
        step for step in job["steps"] if step.get("run", "").startswith("cargo clippy ")
    ]
    assert len(commands) == 1, "expected one executable Clippy command"
    command = commands[0]
    # Every matrix profile's flags (the slim server's included) reach the
    # single lint command, where warnings are denied.
    assert "${{ matrix.profile.flags }}" in command["run"]
    assert command["run"].endswith("--all-targets -- -D warnings")
    assert "if" not in command and not command.get("continue-on-error", False)
    assert not job.get("continue-on-error", False)


def test_engine_main_wheel_publishes_a_prerelease_on_main_push_only(release_matrix):
    workflow, _ = release_matrix
    job = workflow["jobs"]["engine-main-wheel"]
    assert "push" in job["if"] and "refs/heads/main" in job["if"]
    assert "gates-variants" in job["needs"]
    assert job["env"]["EG_WHEEL_TARGET"] == "x86_64-unknown-linux-gnu"
    names = [str(step.get("name", "")) for step in job["steps"]]
    assert any("Smoke test" in name for name in names)
    assert any("SHA256SUMS" in str(step.get("run", "")) for step in job["steps"])
    publish = next(
        step for step in job["steps"] if str(step.get("name", "")).startswith("Publish")
    )
    assert "gh release create engine-main" in publish["run"]
    assert "--prerelease" in publish["run"]
