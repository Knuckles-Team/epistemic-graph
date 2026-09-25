"""kiss drops a whole config table on one unknown key; the gate refuses it.

kiss 0.4.12 silently discards ``[global]`` when it holds the pre-0.4.11
``orphan_module_enabled`` key, re-enabling ``doc == 0`` on every ``///``.
"""

from __future__ import annotations

from pathlib import Path

import kiss_config_keys
import pytest

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]


def test_the_repository_config_uses_only_keys_kiss_knows() -> None:
    kiss_config_keys.require_known_keys(REPO / ".config/kiss.toml")
    assert kiss_config_keys.main([str(REPO / ".config/kiss.toml")]) == 0


def test_the_renamed_orphan_key_is_refused(tmp_path: Path, capsys) -> None:
    config = tmp_path / "kiss.toml"
    config.write_text(
        "[global]\ndocs_allowed = ['./']\norphan_module_enabled = true\n",
        encoding="utf-8",
    )
    assert kiss_config_keys.main([str(config)]) == 2
    assert "global.orphan_module_enabled" in capsys.readouterr().err


def test_every_dropped_table_key_and_a_gate_table_are_named() -> None:
    document = {
        "global": {"docs_allowed": [], "orphan_module_enabled": True},
        "test": {"orphan_detection": True, "bogus": 1},
        "gate": {},
        "rust": {"anything": 1},
    }
    assert kiss_config_keys.unknown_keys(document) == [
        "global.orphan_module_enabled",
        "test.bogus",
        kiss_config_keys.RENAMED_GATE,
    ]


def test_an_unreadable_config_is_refused(tmp_path: Path) -> None:
    config = tmp_path / "kiss.toml"
    config.write_text("[global\n", encoding="utf-8")
    with pytest.raises(kiss_config_keys.UnknownKissKey, match="cannot read"):
        kiss_config_keys.require_known_keys(config)
