"""Opt-in serial execution of the exact-artifact release campaigns.

The harnesses own every engine process and receive one explicit artifact.  The test
never locates or builds an executable and never starts campaigns concurrently.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.exact_artifact

ROOT = Path(__file__).resolve().parents[1]
CAMPAIGNS = (
    (
        "protocol-authorization",
        ROOT / "scripts" / "certify_exact_protocol_authorization.py",
        {"data_path_cases": 14, "protocol_cases": 10, "status": "pass"},
    ),
    (
        "multimodal",
        ROOT / "scripts" / "certify_exact_multimodal.py",
        {
            "dimensions_per_modality": 12,
            "fault_cases": 16,
            "modalities": 4,
            "passed": 4,
            "status": "pass",
        },
    ),
    (
        "knowledge-batch",
        ROOT / "scripts" / "certify_exact_knowledge_batch.py",
        {"families": 7, "requirements": 7, "snapshot_cases": 7, "status": "pass"},
    ),
    (
        "reasoning-repair",
        ROOT / "scripts" / "certify_exact_reasoning_repair.py",
        {"cases": 9, "status": "pass"},
    ),
)


@pytest.mark.timeout(4800)
def test_exact_release_campaigns_are_serial_and_complete(tmp_path: Path) -> None:
    binary = str(os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY", "") or "").strip()
    digest = str(os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY_SHA256", "") or "").strip()
    performance_evidence = str(
        os.environ.get("EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE", "") or ""
    ).strip()
    performance_digest = str(
        os.environ.get("EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE_SHA256", "") or ""
    ).strip()
    if not binary and not digest:
        pytest.skip("exact-artifact release certification was not requested")
    assert binary, "EPISTEMIC_GRAPH_TEST_BINARY is required for exact certification"
    assert digest, (
        "EPISTEMIC_GRAPH_TEST_BINARY_SHA256 is required for exact certification"
    )
    assert performance_evidence, (
        "EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE is required for exact certification"
    )
    assert performance_digest, (
        "EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE_SHA256 is required for exact certification"
    )

    for name, harness, expected_summary in CAMPAIGNS:
        evidence_path = tmp_path / f"exact-{name}.json"
        command = [
            sys.executable,
            str(harness),
            "--binary",
            binary,
            "--binary-sha256",
            digest,
            "--output",
            str(evidence_path),
        ]
        if name == "multimodal":
            command.extend(
                (
                    "--performance-evidence",
                    performance_evidence,
                    "--performance-evidence-sha256",
                    performance_digest,
                )
            )
        completed = subprocess.run(  # noqa: S603 - fixed harness, explicit artifact
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=1100,
        )
        assert completed.returncode == 0, f"{name} exact certification failed"
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        assert evidence["binary"]["sha256"] == digest
        for key, expected in expected_summary.items():
            assert evidence["summary"][key] == expected
        if name == "multimodal":
            assert evidence["binary"]["sealed_copy_verified"] is True
            assert evidence["performance"]["report_sha256"] == performance_digest
            assert all(row["component_tck_not_applicable"] == 0 for row in evidence["matrix"])
