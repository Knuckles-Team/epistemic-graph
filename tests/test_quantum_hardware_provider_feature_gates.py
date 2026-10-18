"""Quantum hardware provider adapters stay behind their own Cargo features.

Static-assertion checks over the eg-quantum-hardware crate manifest, its
module gates in ``src/lib.rs``, and the root package's ``quantum-hardware``
facade feature. They do not replace the native clippy (dead-code) and
provider test-suite evidence.
"""

import re
from pathlib import Path

import pytest
import tomllib

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]
CRATE = REPO / "crates" / "eg-quantum-hardware"
PROVIDERS = ("ibm", "braket", "azure")

# Every module of eg-quantum-hardware exists solely to serve the ibm/braket/
# azure provider adapters, so the whole set compiles under the same gate as
# the adapters themselves. `registry` owns the impl_hardware_backend macro,
# its re-export, and the HardwareResult alias.
PROVIDER_ONLY_MODULES = ("credentials", "error", "quota", "registry", "transport")


def _gated_modules(source: str) -> set[str]:
    """Module declarations compiled only under the three-provider feature gate."""
    return {
        match.group(1)
        for match in re.finditer(
            r"#\[cfg\(any\(\s*"
            r'feature\s*=\s*"ibm"\s*,\s*'
            r'feature\s*=\s*"braket"\s*,\s*'
            r'feature\s*=\s*"azure"\s*\)\s*\)\]\s*\n'
            r"(?:pub(?:\s*\(\s*\w+\s*\))?\s+)?"
            r"mod\s+(\w+)\s*;",
            source,
        )
    }


@pytest.mark.spec("EG-CONTRACT-R042")
def test_provider_adapters_are_feature_gated_and_off_by_default() -> None:
    # (1) Crate manifest: the three providers are opt-in features, and a
    # default build enables none of them (the network dependencies stay
    # optional behind the shared transport feature).
    manifest = tomllib.loads((CRATE / "Cargo.toml").read_text(encoding="utf-8"))
    features = manifest["features"]
    assert features.get("default") == [], "provider adapters must be off by default"
    for provider in PROVIDERS:
        assert provider in features, f"{provider} feature must be declared"
        assert "reqwest-transport" in features[provider], (
            f"{provider} must pull the shared optional HTTP transport"
        )
    assert features["reqwest-transport"] == ["dep:reqwest"]

    # (2) Module gates: every provider-only module -- including registry,
    # which carries the impl_hardware_backend macro, its re-export, and the
    # HardwareResult alias -- compiles only when a provider feature is on,
    # so a default build cannot misreport them as dead code.
    source = (CRATE / "src" / "lib.rs").read_text(encoding="utf-8")
    assert _gated_modules(source) == set(PROVIDER_ONLY_MODULES)
    registry = (CRATE / "src" / "registry.rs").read_text(encoding="utf-8")
    assert re.search(r"macro_rules!\s*impl_hardware_backend\s*\{", registry)
    assert re.search(r"pub\(crate\)\s+type\s+HardwareResult\s*=", registry)
    assert re.search(r"pub\(crate\)\s+use\s+impl_hardware_backend\s*;", registry)

    # (3) Root facade: one feature turns on the crate and all three
    # providers together.
    root = tomllib.loads((REPO / "Cargo.toml").read_text(encoding="utf-8"))
    facade = root["features"]["quantum-hardware"]
    assert "dep:eg-quantum-hardware" in facade
    for provider in PROVIDERS:
        assert f"eg-quantum-hardware/{provider}" in facade
