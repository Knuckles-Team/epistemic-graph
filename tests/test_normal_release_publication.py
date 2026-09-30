"""The normal release preserves its full platform DAG and exact artifact identity."""

import importlib.util
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "verify_release_tag", ROOT / "scripts/verify_release_tag.py"
)
TAG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TAG)


class ReleasePublication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load(
            (ROOT / ".github/workflows/release.yml").read_text()
        )

    def test_platforms_and_required_gates_preserved(self):
        jobs = self.workflow["jobs"]
        legs = jobs["build"]["strategy"]["matrix"]["include"]
        self.assertEqual(
            {leg["name"] for leg in legs},
            {"linux-aarch64", "windows-x86_64", "macos-aarch64", "macos-x86_64"},
        )
        self.assertTrue(all(leg["runner"] != "self-hosted" for leg in legs))
        self.assertEqual(jobs["build"]["needs"], jobs["build-x86"]["needs"])
        self.assertEqual(len(jobs["build-x86"]["needs"]), 8)
        self.assertEqual(
            jobs["build-x86"]["uses"],
            "Knuckles-Team/epistemic-graph/.github/workflows/eg-release-x86.yml@c62f10d90c034e87936be03091ba8d7e13879d5f",
        )
        for name in ("docker-image", "publish-pypi"):
            self.assertEqual(jobs[name]["needs"], ["build", "build-x86"])
        self.assertEqual(
            jobs["publish-image"]["needs"], ["docker-image", "publish-pypi"]
        )

    def test_same_run_artifacts_and_no_blind_skip(self):
        jobs = self.workflow["jobs"]
        for name in ("docker-image", "publish-pypi", "publish-image"):
            self.assertIn("github.event_name == 'push'", jobs[name]["if"])
            for step in jobs[name]["steps"]:
                if step.get("uses", "").startswith("actions/download-artifact@"):
                    self.assertNotIn("run-id", step.get("with", {}))
                    self.assertNotIn("github-token", step.get("with", {}))
        steps = jobs["publish-pypi"]["steps"]
        scripts = "\n".join(step.get("run", "") for step in steps)
        for required in (
            "manylinux_2_28_x86_64",
            "manylinux_2_28_aarch64",
            "win_amd64",
            "--require-engine-kernel",
            "publication.py preflight",
            "publication.py missing",
            "publication.py postverify",
            "scripts/verify_release_tag.py",
        ):
            self.assertIn(required, scripts)
        self.assertNotIn("--skip-existing", scripts)
        image = jobs["publish-image"]["steps"]
        check = next(
            i
            for i, step in enumerate(image)
            if "verify_release_tag.py" in step.get("run", "")
        )
        push = next(
            i
            for i, step in enumerate(image)
            if step.get("with", {}).get("push") is True
        )
        self.assertLess(check, push)

    def test_annotated_and_lightweight_tag(self):
        ref = "refs/tags/v2.27.0"
        source = "a" * 40
        TAG.verify(source + "\t" + ref, ref, source)
        TAG.verify(
            "b" * 40 + "\t" + ref + "\n" + source + "\t" + ref + "^{}", ref, source
        )
        for lines in (
            "",
            "b" * 40 + "\t" + ref,
            source + "\t" + ref + "\n" + source + "\t" + ref,
        ):
            with self.assertRaises(ValueError):
                TAG.verify(lines, ref, source)
        with self.assertRaises(ValueError):
            TAG.verify(source + "\trefs/heads/main", "refs/heads/main", source)


if __name__ == "__main__":
    unittest.main()
