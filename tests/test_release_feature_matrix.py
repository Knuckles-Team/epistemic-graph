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
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    manifests = {
        package: tomllib.loads((ROOT / "crates" / package / "Cargo.toml").read_text())
        for package in PACKAGES
    }
    return workflow, manifests


def _crates_step(workflow):
    # Select executable cargo-test arguments, not a step label or a comment
    # elsewhere in the workflow. Fail if the job loses or duplicates this gate.
    matches = []
    for step in workflow["jobs"]["gates-crates"]["steps"]:
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
    steps = workflow["jobs"]["gates-crates"]["steps"]
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
