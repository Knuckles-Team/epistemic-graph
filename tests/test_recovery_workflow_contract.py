import re
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/recovery"))
from render_dispatch import render


class RecoveryBoundary(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.safe_load(
            (ROOT / ".github/workflows/eg-release-recovery.yml").read_text()
        )
        self.job = self.workflow["jobs"]["wheel"]

    def admits(self, **changes):
        context = dict(
            repository="Knuckles-Team/epistemic-graph",
            event_name="workflow_dispatch",
            ref="refs/heads/main",
            workflow_ref="Knuckles-Team/epistemic-graph/.github/workflows/eg-release-recovery-dispatch.yml@refs/heads/main",
        )
        context.update(changes)
        expression = self.job["if"].replace("&&", " and ")
        return eval(
            expression, {"__builtins__": {}}, {"github": SimpleNamespace(**context)}
        )

    def test_manual_canonical_caller_only(self):
        self.assertTrue(self.admits())
        for change in (
            {"repository": "attacker/epistemic-graph"},
            {"ref": "refs/tags/v2.27.0"},
            {"ref": "refs/heads/feature"},
            {
                "workflow_ref": (
                    "Knuckles-Team/epistemic-graph/.github/workflows/"
                    "other.yml@refs/heads/main"
                )
            },
        ):
            with self.subTest(change=change):
                self.assertFalse(self.admits(**change))

    def test_same_repository_and_fork_pr_events_fail(self):
        # A fork PR reports the base repository; checking repository alone is unsafe.
        for event in (
            "pull_request",
            "pull_request_target",
            "workflow_run",
            "push",
            "schedule",
        ):
            with self.subTest(event=event):
                self.assertFalse(self.admits(event_name=event))

    def test_executable_source_is_fixed_independent_of_caller(self):
        checkout = self.job["steps"][0]
        sha = "29301642c205adc75d63cde2e69d7f53d85d707f"
        self.assertEqual(checkout["with"]["ref"], sha)
        self.assertEqual(
            checkout["with"]["repository"], "Knuckles-Team/epistemic-graph"
        )
        self.assertIs(checkout["with"]["persist-credentials"], False)
        self.assertIn("git rev-parse HEAD", self.job["steps"][1]["run"])
        self.assertIn(sha, self.job["steps"][1]["run"])
        self.assertEqual(self.workflow["on"], {"workflow_call": {}})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        text = yaml.safe_dump(self.workflow)
        for unsafe in (
            "inputs.",
            "secrets.",
            "download-artifact",
            "rust-cache@",
            "actions/cache@",
            "twine upload",
            "push: true",
        ):
            self.assertNotIn(unsafe, text)

    def test_wheel_proof_remains(self):
        steps = self.job["steps"]
        folds = [s for s in steps if s.get("uses") == "./.github/actions/folded-wheel"]
        self.assertEqual(
            [s["with"]["pass"] for s in folds], ["primary", "reproduction"]
        )
        self.assertTrue(
            any("compare_wheel_reproducibility.py" in s.get("run", "") for s in steps)
        )
        self.assertTrue(
            any("epistemic_graph.engine" in s.get("run", "") for s in steps)
        )
        self.assertEqual(self.job["runs-on"]["group"], "eg-release-recovery")
        for step in steps:
            use = step.get("uses", "")
            if use and not use.startswith("./"):
                self.assertRegex(use, r"@[0-9a-f]{40}$")

    def test_exact_source_composite_environment_contract(self):
        composite = (ROOT / ".github/actions/folded-wheel/action.yml").read_text()
        required = set(re.findall(r"\benv\.([A-Z_]+)\b", composite))
        self.assertTrue(required)
        self.assertLessEqual(required, set(self.job["env"]))
        original = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
        self.assertEqual(
            self.job["env"]["MATURIN_FEATURES"], original["env"]["MATURIN_FEATURES"]
        )
        self.assertEqual(self.job["env"]["MATURIN_FEATURES"], "full,ast-extended")

    def test_active_caller_pins_reviewed_workflow(self):
        template = (
            ROOT / "scripts/recovery/eg-release-recovery-dispatch.yml.in"
        ).read_text()
        caller = ROOT / ".github/workflows/eg-release-recovery-dispatch.yml"
        sha = "4b232785086ae47791f1e58fe075c1ee74e6c877"
        self.assertEqual(caller.read_text(), render(sha, template))
        parsed = yaml.load(caller.read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(parsed["on"]), {"workflow_dispatch"})
        self.assertEqual(parsed["permissions"], {"contents": "read"})
        self.assertEqual(set(parsed["jobs"]["recovery"]), {"uses"})

    def test_caller_template_fails_closed_until_real_sha(self):
        template = (
            ROOT / "scripts/recovery/eg-release-recovery-dispatch.yml.in"
        ).read_text()
        for value in ("main", "v2.27.0", "0" * 40, "a" * 39, "a" * 40 + "\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                render(value, template)
        self.assertIn("@" + "a" * 40, render("a" * 40, template))


if __name__ == "__main__":
    unittest.main()
