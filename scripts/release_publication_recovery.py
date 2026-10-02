"""Verify the fixed 2.27.0 release inputs before a publication-only continuation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import zipfile
from pathlib import Path

PINS = Path(__file__).with_suffix(".json")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def read_json(data: bytes):
    return json.loads(data, object_pairs_hook=unique_object)


def api(path: str):
    return read_json(subprocess.check_output(["gh", "api", path]))


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        checksum = hashlib.sha256()
        while block := stream.read(1024 * 1024):
            checksum.update(block)
        return checksum.hexdigest()


def check_dispatch(env: dict[str, str], pins: dict) -> None:
    expected = {
        "GITHUB_REPOSITORY": pins["repository"],
        "GITHUB_REPOSITORY_ID": str(pins["repository_id"]),
        "GITHUB_REPOSITORY_OWNER_ID": str(pins["owner_id"]),
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_ACTOR_ID": str(pins["actor_id"]),
        "GITHUB_ACTOR": "Knucklessg1",
        "GITHUB_TRIGGERING_ACTOR": "Knucklessg1",
        "GITHUB_WORKFLOW_REF": pins["repository"]
        + "/.github/workflows/release-publication-recovery.yml@refs/heads/main",
    }
    require(all(env.get(k) == v for k, v in expected.items()), "untrusted dispatch")
    require(
        env.get("GITHUB_ACTOR") == env.get("GITHUB_TRIGGERING_ACTOR"),
        "rerun actor changed",
    )


def check_run(run: dict, pins: dict) -> None:
    expected = {
        "id": pins["run_id"],
        "run_attempt": pins["run_attempt"],
        "head_sha": pins["source"],
        "event": "push",
        "head_branch": pins["tag"],
        "path": ".github/workflows/release.yml",
        "status": "completed",
    }
    require(all(run.get(k) == v for k, v in expected.items()), "release run changed")
    require(run["repository"]["id"] == pins["repository_id"], "wrong run repository")
    require(run["actor"]["id"] == pins["actor_id"], "wrong release actor")
    require(
        run["triggering_actor"]["id"] == pins["actor_id"], "wrong release rerun actor"
    )


def check_job(job: dict, pins: dict, wheel: bool = False) -> None:
    expected = {
        "run_id": pins["run_id"],
        "run_attempt": pins["run_attempt"],
        "head_sha": pins["source"],
        "status": "completed",
        "conclusion": "success",
    }
    require(
        all(job.get(k) == v for k, v in expected.items()), "required job not successful"
    )
    if wheel:
        steps = {s["name"]: s["conclusion"] for s in job["steps"]}
        for name in (
            "Smoke test (server --help + import epistemic_graph.{numeric,engine})",
            "Upload wheel artifact",
        ):
            require(steps.get(name) == "success", "wheel smoke/upload proof missing")


def check_artifact(artifact: dict, expected: dict, pins: dict) -> None:
    require(
        artifact["id"] == expected["id"] and artifact["name"] == expected["name"],
        "wrong artifact",
    )
    require(artifact["expired"] is False, "artifact expired")
    require(
        artifact["digest"] == "sha256:" + expected["archive_sha256"],
        "archive identity changed",
    )
    run = artifact["workflow_run"]
    require(
        run["id"] == pins["run_id"] and run["head_sha"] == pins["source"],
        "wrong artifact source",
    )
    require(run["repository_id"] == pins["repository_id"], "wrong artifact repository")
    require(
        run["head_repository_id"] == pins["repository_id"],
        "foreign artifact repository",
    )


def unpack(archive: Path, destination: Path, expected: dict) -> Path:
    require(digest(archive) == expected["archive_sha256"], "download digest mismatch")
    with zipfile.ZipFile(archive) as bundle:
        entries = bundle.infolist()
        require(len(entries) == 1, "unexpected/duplicate archive members")
        entry = entries[0]
        require(entry.filename == expected["filename"], "unexpected wheel filename")
        require(entry.file_size == expected["bytes"], "unexpected wheel size")
        mode = entry.external_attr >> 16
        require(stat.S_IFMT(mode) in (0, stat.S_IFREG), "nonregular archive member")
        target = destination / expected["filename"]
        with bundle.open(entry) as source, target.open("xb") as output:
            while block := source.read(1024 * 1024):
                output.write(block)
    require(digest(target) == expected["sha256"], "wheel digest mismatch")
    with zipfile.ZipFile(target) as wheel:
        receipts = [
            i
            for i in wheel.infolist()
            if i.filename == "epistemic_graph/contract/receipt.json"
        ]
        require(len(receipts) == 1, "missing/duplicate contract receipt")
        require(
            hashlib.sha256(wheel.read(receipts[0])).hexdigest()
            == expected["receipt_sha256"],
            "contract receipt changed",
        )
    return target


def check_tag(pins: dict) -> None:
    base = "repos/" + pins["repository"] + "/git/"
    ref = api(base + "ref/tags/" + pins["tag"])
    require(ref["object"]["type"] == "tag", "release tag is not annotated")
    require(ref["object"]["sha"] == pins["tag_object"], "release tag object moved")
    tag = api(base + "tags/" + pins["tag_object"])
    require(
        tag["object"]
        == {"sha": pins["source"], "type": "commit", "url": tag["object"]["url"]},
        "release tag source moved",
    )


def check_staged(directory: Path, pins: dict) -> None:
    require(not directory.is_symlink(), "linked staging directory")
    expected = {a["filename"]: a for a in pins["artifacts"]}
    require(
        {p.name for p in directory.iterdir()} == set(expected),
        "staged platform set changed",
    )
    for name, item in expected.items():
        path = directory / name
        require(
            path.is_file() and not path.is_symlink() and path.stat().st_nlink == 1,
            "linked/nonregular staged wheel",
        )
        require(
            path.stat().st_size == item["bytes"] and digest(path) == item["sha256"],
            "staged bytes changed",
        )


def prepare(directory: Path, evidence: Path, pins: dict) -> None:
    check_dispatch(dict(os.environ), pins)
    check_tag(pins)
    prefix = "repos/" + pins["repository"] + "/actions/"
    run = api(prefix + f"runs/{pins['run_id']}/attempts/{pins['run_attempt']}")
    check_run(run, pins)
    jobs = []
    wheel_jobs = {a["job_id"] for a in pins["artifacts"]}
    for job_id in pins["required_jobs"] + sorted(wheel_jobs):
        job = api(prefix + f"jobs/{job_id}")
        require(job["id"] == job_id, "wrong job identity")
        check_job(job, pins, job_id in wheel_jobs)
        jobs.append(
            {k: job[k] for k in ("id", "name", "conclusion", "head_sha", "run_attempt")}
        )
    directory.mkdir(exist_ok=False)
    archives = evidence.parent / "archives"
    archives.mkdir(exist_ok=False)
    artifacts = []
    for item in pins["artifacts"]:
        metadata = api(prefix + f"artifacts/{item['id']}")
        check_artifact(metadata, item, pins)
        archive = archives / f"{item['id']}.zip"
        with archive.open("xb") as output:
            subprocess.run(
                ["gh", "api", prefix + f"artifacts/{item['id']}/zip"],
                stdout=output,
                check=True,
            )
        unpack(archive, directory, item)
        artifacts.append({**item, "expires_at": metadata["expires_at"]})
    check_staged(directory, pins)
    evidence.write_text(
        json.dumps(
            {
                "run": pins["run_id"],
                "attempt": pins["run_attempt"],
                "source": pins["source"],
                "jobs": jobs,
                "artifacts": artifacts,
            },
            indent=2,
        )
        + "\n"
    )


def quota_gate(mib: int, evidence_url: str, pins: dict) -> None:
    import re

    require(mib >= 500, "verified quota must be at least 500 MiB")
    require(
        re.fullmatch(
            r"https://github.com/pypi/support/issues/12510#issuecomment-[0-9]+",
            evidence_url,
        )
        is not None,
        "quota approval evidence required",
    )
    require(
        all(a["bytes"] <= mib * 1024 * 1024 for a in pins["artifacts"]),
        "wheel exceeds verified quota",
    )


def verify_quota_evidence(url: str) -> None:
    comment_id = url.rsplit("-", 1)[1]
    comment = api("repos/pypi/support/issues/comments/" + comment_id)
    require(
        comment["issue_url"]
        == "https://api.github.com/repos/pypi/support/issues/12510",
        "quota evidence belongs to another issue",
    )
    require(
        comment["author_association"] in {"OWNER", "MEMBER", "COLLABORATOR"},
        "quota evidence is not from the support repository team",
    )
    require(bool(comment["body"].strip()), "empty quota evidence")
    # The dispatching operator must verify that this comment approves the limit.
    # PyPI does not expose the project's private quota through its public JSON API.
    Path("quota-approval.json").write_text(json.dumps(comment, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "recheck", "quota"))
    parser.add_argument("--directory", type=Path, default=Path("dist"))
    parser.add_argument("--evidence", type=Path, default=Path("evidence.json"))
    parser.add_argument("--quota-mib", type=int, default=0)
    parser.add_argument("--quota-evidence", default="")
    args = parser.parse_args()
    pins = read_json(PINS.read_bytes())
    if args.mode == "prepare":
        prepare(args.directory, args.evidence, pins)
    elif args.mode == "recheck":
        check_dispatch(dict(os.environ), pins)
        check_tag(pins)
        check_staged(args.directory, pins)
    else:
        quota_gate(args.quota_mib, args.quota_evidence, pins)
        verify_quota_evidence(args.quota_evidence)


if __name__ == "__main__":
    main()
