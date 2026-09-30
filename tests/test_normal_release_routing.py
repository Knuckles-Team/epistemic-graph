"""Light trust and wheel-contract tests; no network or native compilation."""

import io
import json
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/eg-release-x86.yml"
SHA = "a" * 40


def load_contract():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    source = workflow["jobs"]["authorize"]["steps"][0]["run"]
    code = source.split("python3 - <<'PYTHON'\n", 1)[1].rsplit("PYTHON", 1)[0]
    namespace = {"__name__": "tested_workflow_guard"}
    exec(compile(code, str(WORKFLOW), "exec"), namespace)
    return workflow, namespace


def context():
    return dict(
        repository="Knuckles-Team/epistemic-graph",
        repository_id="1248541673",
        repository_owner_id="119072828",
        event_name="push",
        ref="refs/tags/v2.27.0",
        ref_type="tag",
        workflow_ref="Knuckles-Team/epistemic-graph/.github/workflows/release.yml@refs/tags/v2.27.0",
        actor="Knucklessg1",
        actor_id="8661571",
        triggering_actor="Knucklessg1",
        sender_id=8661571,
        deleted=False,
        sha=SHA,
    )


class NormalReleaseTrust(unittest.TestCase):
    workflow: dict[str, Any]
    guard: dict[str, Any]

    @classmethod
    def setUpClass(cls):
        cls.workflow, cls.guard = load_contract()

    def test_approved_push_and_source(self):
        self.assertEqual(self.guard["authorize"](context(), SHA), SHA)

    def test_untrusted_contexts_rejected(self):
        changes: list[tuple[str, Any]] = [
            ("event_name", event)
            for event in (
                "pull_request",
                "pull_request_target",
                "workflow_run",
                "workflow_dispatch",
            )
        ]
        changes += [
            ("repository", "fork/epistemic-graph"),
            ("repository_id", "1"),
            ("repository_owner_id", "1"),
            ("ref", "refs/heads/main"),
            ("ref", "refs/tags/v2.28.0"),
            ("ref_type", "branch"),
            ("workflow_ref", "another/caller"),
            ("actor", "attacker"),
            ("actor_id", "1"),
            ("triggering_actor", "attacker"),
            ("sender_id", 1),
            ("deleted", True),
            ("sha", "not-a-sha"),
        ]
        for field, value in changes:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.guard["authorize"](dict(context(), **{field: value}), SHA)
        with self.assertRaises(ValueError):
            self.guard["authorize"](context(), "b" * 40)

    def test_remote_annotated_tag_is_peeled(self):
        replies = [
            dict(object=dict(type="tag", sha="b" * 40)),
            dict(object=dict(type="commit", sha=SHA)),
        ]
        with patch(
            "urllib.request.urlopen",
            side_effect=[io.StringIO(json.dumps(x)) for x in replies],
        ) as fetch:
            self.assertEqual(self.guard["remote_tag_commit"]("synthetic-token"), SHA)
        self.assertEqual(fetch.call_count, 2)
        for call in fetch.call_args_list:
            self.assertTrue(
                call.args[0].full_url.startswith(
                    "https://api.github.com/repos/Knuckles-Team/epistemic-graph/git/"
                )
            )
            self.assertNotIn("/heads/main", call.args[0].full_url)

    def test_remote_wrong_type_and_cycles_fail(self):
        for obj in (dict(type="tree", sha=SHA), dict(type="tag", sha="bad")):
            with (
                patch(
                    "urllib.request.urlopen",
                    return_value=io.StringIO(json.dumps({"object": obj})),
                ),
                self.assertRaises(ValueError),
            ):
                self.guard["remote_tag_commit"]("synthetic-token")
        replies = [
            io.StringIO(json.dumps({"object": dict(type="tag", sha=SHA)}))
            for _ in range(9)
        ]
        with (
            patch("urllib.request.urlopen", side_effect=replies),
            self.assertRaises(ValueError),
        ):
            self.guard["remote_tag_commit"]("synthetic-token")

    def test_no_caller_inputs_or_publication_permissions(self):
        self.assertEqual(self.workflow["on"], {"workflow_call": {}})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        job = self.workflow["jobs"]["wheel"]
        self.assertEqual(job["needs"], "authorize")
        self.assertEqual(job["runs-on"]["group"], "eg-release-recovery")
        for expected in (
            "needs.authorize.outputs.source == github.sha",
            "github.actor_id == '8661571'",
            "github.event_name == 'push'",
        ):
            self.assertIn(expected, job["if"])
        self.assertNotIn("secrets", self.workflow)

    def test_exact_source_and_existing_wheel_steps(self):
        jobs = self.workflow["jobs"]
        self.assertEqual(jobs["wheel"]["steps"], jobs["wheel-hosted"]["steps"])
        steps = jobs["wheel"]["steps"]
        checkout = next(
            step
            for step in steps
            if step.get("uses", "").startswith("actions/checkout@")
        )
        self.assertEqual(checkout["with"]["ref"], "${{ github.sha }}")
        self.assertFalse(checkout["with"]["persist-credentials"])
        self.assertEqual(
            steps[0]["name"], "Recheck canonical tag before self-hosted checkout"
        )
        self.assertEqual(
            steps[2]["name"], "Verify exact event source before repository scripts"
        )
        self.assertEqual(jobs["wheel"]["env"]["MATURIN_FEATURES"], "full,ast-extended")
        self.assertEqual(jobs["wheel"]["env"]["EG_WHEEL_JOBS_ARG"], "--jobs 2")
        upload = next(
            step
            for step in steps
            if step.get("uses", "").startswith("actions/upload-artifact@")
        )
        self.assertEqual(upload["with"]["name"], "wheel-linux-x86_64")
        self.assertEqual(upload["with"]["if-no-files-found"], "error")
        passes = [
            step["with"]["pass"]
            for step in steps
            if step.get("uses") == "./.github/actions/folded-wheel"
        ]
        self.assertEqual(passes, ["primary", "reproduction"])


if __name__ == "__main__":
    unittest.main()
