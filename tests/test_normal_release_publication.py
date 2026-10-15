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
        # The tag pipeline builds only: ci.yml already ran every gate, and
        # `guard` proves it concluded success for the tagged commit.
        self.assertEqual(jobs["build"]["needs"], "guard")
        self.assertEqual(jobs["build-x86"]["needs"], "guard")
        self.assertEqual(jobs["build-sdist"]["needs"], "guard")
        self.assertFalse(
            [name for name in jobs if name.startswith("gates") or name == "security"]
        )
        self.assertEqual(
            jobs["build-x86"]["uses"],
            "Knuckles-Team/epistemic-graph/.github/workflows/eg-release-x86.yml@bc4448160e8d16deb8210ce8aeb058b2da293d93",
        )
        self.assertNotIn("docker-image", jobs)
        self.assertNotIn("publish-image", jobs)
        self.assertEqual(
            jobs["publish-pypi"]["needs"], ["build", "build-x86", "build-sdist"]
        )

    def test_source_archive_is_validated_before_upload(self):
        jobs = self.workflow["jobs"]
        source = jobs["build-sdist"]
        self.assertEqual(source["permissions"], {"contents": "read"})
        self.assertNotIn("continue-on-error", source)
        source_steps = source["steps"]
        build = next(
            index
            for index, step in enumerate(source_steps)
            if "maturin sdist" in step.get("run", "")
        )
        command = source_steps[build]["run"]
        self.assertIn("scripts/check_sdist_completeness.py", command)
        self.assertLess(
            command.index("maturin sdist"),
            command.index("scripts/check_sdist_completeness.py"),
        )
        upload = next(
            index
            for index, step in enumerate(source_steps)
            if step.get("uses", "").startswith("actions/upload-artifact@")
        )
        self.assertLess(build, upload)
        self.assertEqual(source_steps[upload]["with"]["if-no-files-found"], "error")
        self.assertFalse(any(step.get("continue-on-error") for step in source_steps))

    def test_source_archive_is_validated_before_publication(self):
        publish = self.workflow["jobs"]["publish-pypi"]["steps"]
        download = next(
            index
            for index, step in enumerate(publish)
            if step.get("with", {}).get("name") == "source-distribution"
        )
        validate = next(
            index
            for index, step in enumerate(publish)
            if "scripts/check_sdist_completeness.py" in step.get("run", "")
        )
        freeze = next(
            index
            for index, step in enumerate(publish)
            if "publication.py preflight" in step.get("run", "")
        )
        self.assertLess(download, validate)
        self.assertLess(validate, freeze)

    def test_storage_crash_processes_do_not_overlap_sibling_file_tests(self):
        steps = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"][
            "gates-crates"
        ]["steps"]
        commands = [step.get("run", "") for step in steps]
        storage = (
            "python3 scripts/cargo_test_rescue.py cargo test --locked "
            "-p eg-storage --all-features --no-fail-fast -- --test-threads=1"
        )
        self.assertEqual(commands.count(storage), 1)
        workspace = next(
            command for command in commands if "--locked --workspace" in command
        )
        self.assertIn("--exclude eg-storage ", workspace)
        self.assertNotIn("--test-threads=1", workspace)
        self.assertLess(commands.index(storage), commands.index(workspace))

    def test_same_run_artifacts_and_no_blind_skip(self):
        jobs = self.workflow["jobs"]
        for name in ("publish-pypi",):
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
            "unexpected epistemic-graph-server executable",
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
