"""The normal release preserves its full platform DAG and exact artifact identity."""

import importlib.util
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "verify_release_tag", ROOT / "scripts/verify_release_tag.py"
)
assert SPEC is not None and SPEC.loader is not None
TAG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TAG)

METADATA_SPEC = importlib.util.spec_from_file_location(
    "release_build_metadata", ROOT / "scripts/release_build_metadata.py"
)
assert METADATA_SPEC is not None and METADATA_SPEC.loader is not None
METADATA = importlib.util.module_from_spec(METADATA_SPEC)
METADATA_SPEC.loader.exec_module(METADATA)


class ReleasePublication(unittest.TestCase):
    workflow: dict[str, Any]

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
            "Knuckles-Team/epistemic-graph/.github/workflows/eg-release-x86.yml@bc4448160e8d16deb8210ce8aeb058b2da293d93",
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

    def test_release_metadata_and_staging(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pin = root / "rust.toml"
            pin.write_text('[toolchain]\nchannel = "1.96.0"\n')
            self.assertEqual(METADATA.rust_pin(pin), "1.96.0")
            pin.write_text('[toolchain]\nchannel = "stable"\n')
            with self.assertRaises(ValueError):
                METADATA.rust_pin(pin)
            source = root / "primary"
            source.mkdir()
            with self.assertRaises(ValueError):
                METADATA.stage(source, root / "dist")
            wheel = source / "epistemic_graph-2.27.0-py3-none-any.whl"
            wheel.write_bytes(b"exact primary wheel")
            METADATA.stage(source, root / "dist")
            self.assertEqual(
                (root / "dist" / wheel.name).read_bytes(), wheel.read_bytes()
            )
            with self.assertRaises(FileExistsError):
                METADATA.stage(source, root / "dist")
        with patch.object(METADATA.subprocess, "check_output", return_value="1234\n"):
            self.assertEqual(METADATA.source_time(), "1234")
        with patch.object(METADATA.subprocess, "check_output", return_value="bad\n"):
            with self.assertRaises(ValueError):
                METADATA.source_time()

    def test_reproduction_comparison_precedes_staging(self):
        with (
            patch.object(METADATA.subprocess, "run") as compare,
            patch.object(METADATA, "stage") as stage,
        ):
            METADATA.verify_and_stage(False)
            compare.assert_not_called()
            stage.assert_called_once()
        with (
            patch.object(
                METADATA.subprocess, "run", side_effect=RuntimeError("different bytes")
            ),
            patch.object(METADATA, "stage") as stage,
        ):
            with self.assertRaises(RuntimeError):
                METADATA.verify_and_stage(True)
            stage.assert_not_called()
        with (
            patch.object(METADATA.subprocess, "run") as compare,
            patch.object(METADATA, "stage"),
        ):
            METADATA.verify_and_stage(True)
            self.assertIn(
                "scripts/compare_wheel_reproducibility.py", compare.call_args.args[0]
            )
            self.assertTrue(compare.call_args.kwargs["check"])

    def test_shared_smoke_retains_runtime_checks(self):
        script = (ROOT / "scripts/release_wheel_smoke.sh").read_text()
        for proof in (
            "set -euo pipefail",
            "--force-reinstall",
            "epistemic-graph-server --help",
            "metadata.requires",
            "numpy",
            "n.sum(values)==10.0",
            "import epistemic_graph.engine",
        ):
            self.assertIn(proof, script)
        hosted_steps = self.workflow["jobs"]["build"]["steps"]
        self.assertTrue(
            any(
                step.get("run", "").strip() == "bash scripts/release_wheel_smoke.sh"
                for step in hosted_steps
            )
        )

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
