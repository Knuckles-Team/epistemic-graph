"""Negative-boundary coverage for the frozen publication continuation."""

import copy
import hashlib
import importlib.util
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "recovery", ROOT / "scripts/release_publication_recovery.py"
)
assert SPEC and SPEC.loader
RECOVERY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECOVERY)
PINS = json.loads((ROOT / "scripts/release_publication_recovery.json").read_text())


class RecoveryTests(unittest.TestCase):
    def test_dispatch_rejects_pr_fork_other_actor_and_non_main(self):
        env = {
            "GITHUB_REPOSITORY": PINS["repository"],
            "GITHUB_REPOSITORY_ID": str(PINS["repository_id"]),
            "GITHUB_REPOSITORY_OWNER_ID": str(PINS["owner_id"]),
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_ACTOR_ID": str(PINS["actor_id"]),
            "GITHUB_ACTOR": "Knucklessg1",
            "GITHUB_TRIGGERING_ACTOR": "Knucklessg1",
            "GITHUB_WORKFLOW_REF": PINS["repository"]
            + "/.github/workflows/release-publication-recovery.yml@refs/heads/main",
        }
        RECOVERY.check_dispatch(env, PINS)
        for key, value in [
            ("GITHUB_EVENT_NAME", "pull_request"),
            ("GITHUB_REPOSITORY_ID", "1"),
            ("GITHUB_REF", "refs/heads/other"),
            ("GITHUB_ACTOR_ID", "1"),
            ("GITHUB_TRIGGERING_ACTOR", "other"),
        ]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                RECOVERY.check_dispatch({**env, key: value}, PINS)

    def test_cancelled_optional_run_does_not_replace_required_job_proof(self):
        job = {
            "run_id": PINS["run_id"],
            "run_attempt": 1,
            "head_sha": PINS["source"],
            "status": "completed",
            "conclusion": "success",
        }
        RECOVERY.check_job(job, PINS)
        for key, value in [
            ("conclusion", "cancelled"),
            ("conclusion", "skipped"),
            ("run_attempt", 2),
            ("head_sha", "0" * 40),
        ]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                RECOVERY.check_job({**job, key: value}, PINS)
        with self.assertRaises(KeyError):
            RECOVERY.check_job(job, PINS, wheel=True)

    def test_artifact_is_bound_to_repository_source_and_digest(self):
        expected = PINS["artifacts"][0]
        item = {
            "id": expected["id"],
            "name": expected["name"],
            "expired": False,
            "digest": "sha256:" + expected["archive_sha256"],
            "workflow_run": {
                "id": PINS["run_id"],
                "head_sha": PINS["source"],
                "repository_id": PINS["repository_id"],
                "head_repository_id": PINS["repository_id"],
            },
        }
        RECOVERY.check_artifact(item, expected, PINS)
        for key, value in [
            ("id", 1),
            ("expired", True),
            ("digest", "sha256:" + "0" * 64),
        ]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                RECOVERY.check_artifact({**item, key: value}, expected, PINS)
        foreign = copy.deepcopy(item)
        foreign["workflow_run"]["head_repository_id"] = 1
        with self.assertRaises(ValueError):
            RECOVERY.check_artifact(foreign, expected, PINS)

    def test_duplicate_json_is_rejected_at_nested_level(self):
        with self.assertRaises(ValueError):
            RECOVERY.read_json(b'{"outer":{"digest":"one","digest":"two"}}')

    def test_quota_defaults_block_upload(self):
        for mib, url in [
            (0, ""),
            (100, "https://github.com/pypi/support/issues/12510#issuecomment-123"),
            (500, "https://example.com/approval"),
        ]:
            with self.subTest(mib=mib, url=url), self.assertRaises(ValueError):
                RECOVERY.quota_gate(mib, url, PINS)
        RECOVERY.quota_gate(
            500, "https://github.com/pypi/support/issues/12510#issuecomment-123", PINS
        )

    def test_archive_members_and_staged_bytes_fail_closed(self):
        receipt = b"{}"
        wheel_buffer = io.BytesIO()
        with zipfile.ZipFile(wheel_buffer, "w") as wheel:
            wheel.writestr("epistemic_graph/contract/receipt.json", receipt)
        wheel_bytes = wheel_buffer.getvalue()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "artifact.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("test.whl", wheel_bytes)
            expected = {
                "filename": "test.whl",
                "bytes": len(wheel_bytes),
                "sha256": hashlib.sha256(wheel_bytes).hexdigest(),
                "archive_sha256": RECOVERY.digest(archive),
                "receipt_sha256": hashlib.sha256(receipt).hexdigest(),
            }
            out = root / "dist"
            out.mkdir()
            target = RECOVERY.unpack(archive, out, expected)
            pins = {"artifacts": [expected]}
            RECOVERY.check_staged(out, pins)
            target.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                RECOVERY.check_staged(out, pins)
            target.unlink()
            target.symlink_to(archive)
            with self.assertRaises(ValueError):
                RECOVERY.check_staged(out, pins)
            for name in ["../test.whl", "/test.whl", "other.whl"]:
                with zipfile.ZipFile(archive, "w") as bundle:
                    bundle.writestr(name, wheel_bytes)
                expected["archive_sha256"] = RECOVERY.digest(archive)
                with self.subTest(name=name), self.assertRaises(ValueError):
                    RECOVERY.unpack(archive, out, expected)

    def test_run_rejects_source_attempt_actor_and_repository_drift(self):
        run = {
            "id": PINS["run_id"],
            "run_attempt": 1,
            "head_sha": PINS["source"],
            "event": "push",
            "head_branch": PINS["tag"],
            "path": ".github/workflows/release.yml",
            "status": "completed",
            "conclusion": "cancelled",
            "repository": {"id": PINS["repository_id"]},
            "actor": {"id": PINS["actor_id"]},
            "triggering_actor": {"id": PINS["actor_id"]},
        }
        RECOVERY.check_run(run, PINS)
        for key, value in [
            ("head_sha", "0" * 40),
            ("run_attempt", 2),
            ("actor", {"id": 1}),
            ("repository", {"id": 1}),
        ]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                RECOVERY.check_run({**run, key: value}, PINS)

    def test_moved_annotated_tag_fails_before_download(self):
        ref = {"object": {"type": "tag", "sha": PINS["tag_object"]}}
        tag = {"object": {"type": "commit", "sha": PINS["source"], "url": "unused"}}
        with patch.object(RECOVERY, "api", side_effect=[ref, tag]):
            RECOVERY.check_tag(PINS)
        for obj in [
            {"type": "commit", "sha": PINS["source"]},
            {"type": "tag", "sha": "0" * 40},
        ]:
            with (
                patch.object(RECOVERY, "api", return_value={"object": obj}),
                self.assertRaises(ValueError),
            ):
                RECOVERY.check_tag(PINS)
        tag["object"]["sha"] = "0" * 40
        with (
            patch.object(RECOVERY, "api", side_effect=[ref, tag]),
            self.assertRaises(ValueError),
        ):
            RECOVERY.check_tag(PINS)

    def test_quota_comment_must_be_real_support_team_evidence(self):
        comment = {
            "issue_url": "https://api.github.com/repos/pypi/support/issues/12510",
            "author_association": "MEMBER",
            "body": "Operator must assess approval text.",
        }
        for key, value in [
            ("issue_url", "https://api.github.com/repos/pypi/support/issues/1"),
            ("author_association", "NONE"),
            ("body", ""),
        ]:
            with (
                patch.object(RECOVERY, "api", return_value={**comment, key: value}),
                self.assertRaises(ValueError),
            ):
                RECOVERY.verify_quota_evidence(
                    "https://github.com/pypi/support/issues/12510#issuecomment-123"
                )

    def test_central_proof_runs_in_frozen_source_with_nested_contract(self):
        workflow = yaml.safe_load(
            (ROOT / ".github/workflows/release-publication-recovery.yml").read_text()
        )
        job = workflow["jobs"]["recover"]
        self.assertEqual(job["runs-on"], "ubuntu-latest")
        self.assertEqual(job["env"]["SOURCE_COMMIT"], PINS["source"])
        steps = job["steps"]
        contract = next(s for s in steps if s.get("with", {}).get("repository"))
        self.assertEqual(contract["with"]["path"], ".frozen-source/.pipeline-contract")
        self.assertEqual(contract["with"]["ref"], PINS["contract_commit"])
        proof = [s for s in steps if "publication.py" in s.get("run", "")]
        self.assertEqual(len(proof), 3)
        for step in proof:
            self.assertEqual(step["working-directory"], ".frozen-source")
            self.assertIn("--directory ../dist", step["run"])
        uploads = next(s for s in steps if "TWINE_PASSWORD" in s.get("env", {}))
        self.assertEqual(uploads["if"], "inputs.publish")
        self.assertEqual(
            workflow["permissions"], {"contents": "read", "actions": "read"}
        )

    def test_manifest_is_exact_three_original_artifacts(self):
        self.assertEqual(
            {a["id"] for a in PINS["artifacts"]},
            {11196548715, 11193437546, 11193049469},
        )
        self.assertEqual(len(PINS["artifacts"]), 3)
        self.assertTrue(all(a["bytes"] > 100 * 1024 * 1024 for a in PINS["artifacts"]))


if __name__ == "__main__":
    unittest.main()
