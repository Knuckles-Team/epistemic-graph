"""EG-CONTRACT-R035: the release workflow's `gates` job runs to completion as
a whole, including its eg-compute `solve` test step, with no gap between a
test target *declared* in the job and one actually *executed*.

`crates/eg-compute/tests/solve.rs` is gated `#![cfg(feature = "solve")]`
(cargo auto-discovers it as the `solve` integration-test binary because the
crate does not set `autotests = false`). If the gates job's cargo-test
invocation for `eg-compute` ever stopped passing `--all-features` (or an
explicit `--features solve`), that whole test binary would compile to zero
tests and the step would report success while covering nothing -- exactly
the "source that no entry point ever compiles" gap this requirement closes.

This audit parses the workflow YAML directly (it does not replace the native
gates job's own compilation/execution evidence); it exists so a future edit
to the gates job's eg-compute step cannot narrow its feature selection and
silently drop `solve` coverage without a test noticing.
"""

import re
import shlex
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
FEATURE_GATE_RE = re.compile(r'#!\[cfg\(feature\s*=\s*"([^"]+)"\)\]')


def _gates_steps():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    return workflow["jobs"]["gates"]["steps"]


def _cargo_test_invocations(steps):
    """Yield (packages, all_features, features) for each `cargo test` step."""
    for step in steps:
        tokens = shlex.split(step.get("run", ""), comments=True)
        if "cargo" not in tokens:
            continue
        idx = tokens.index("cargo")
        if idx + 1 >= len(tokens) or tokens[idx + 1] != "test":
            continue
        packages = [tokens[i + 1] for i, t in enumerate(tokens) if t == "-p"]
        if not packages:
            continue
        all_features = "--all-features" in tokens
        features = []
        if "--features" in tokens:
            raw = tokens[tokens.index("--features") + 1]
            features = raw.replace(",", " ").split()
        yield packages, all_features, features


def _required_feature(test_file: Path) -> str | None:
    match = FEATURE_GATE_RE.search(test_file.read_text())
    return match.group(1) if match else None


@pytest.mark.spec("EG-CONTRACT-R035")
def test_eg_compute_solve_step_enables_the_solve_feature():
    """The one gates-job step covering eg-compute (`Test (lower crates, all
    features)`) must enable the `solve` feature, or `tests/solve.rs` compiles
    to an empty test binary and the eg-compute solve coverage this
    requirement names never actually executes."""
    invocations = [
        inv for inv in _cargo_test_invocations(_gates_steps()) if "eg-compute" in inv[0]
    ]
    assert invocations, "gates job must declare a cargo test invocation covering eg-compute"
    _, all_features, features = invocations[0]
    assert all_features or "solve" in features, (
        "eg-compute's gates-job cargo test step must pass --all-features or "
        "--features solve so crates/eg-compute/tests/solve.rs is not silently skipped"
    )

    solve_test = ROOT / "crates" / "eg-compute" / "tests" / "solve.rs"
    assert solve_test.is_file(), "expected crates/eg-compute/tests/solve.rs to exist"
    assert _required_feature(solve_test) == "solve", (
        "tests/solve.rs is expected to stay gated on the `solve` feature; if that "
        "changes, this audit's coverage mapping must be revisited alongside it"
    )
