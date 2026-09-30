"""Fail publication if the canonical remote tag no longer names this run's source."""

import os
import re
import subprocess


def verify(lines: str, ref: str, source: str) -> None:
    if not re.fullmatch(r"refs/tags/v[0-9][A-Za-z0-9.+_-]*", ref):
        raise ValueError("invalid release tag")
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("invalid source commit")
    rows = [line.split() for line in lines.splitlines()]
    refs = {name: sha for sha, name in rows}
    if len(refs) != len(rows) or ref not in refs or not set(refs) <= {ref, ref + "^{}"}:
        raise ValueError("missing/ambiguous canonical tag")
    if refs.get(ref + "^{}", refs[ref]) != source:
        raise ValueError("canonical release tag moved")


def main() -> None:
    if (
        os.environ.get("GITHUB_EVENT_NAME") != "push"
        or os.environ.get("GITHUB_REPOSITORY") != "Knuckles-Team/epistemic-graph"
    ):
        raise ValueError("publication requires canonical tag push")
    ref, source = os.environ["GITHUB_REF"], os.environ["GITHUB_SHA"]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if head != source:
        raise ValueError("checkout differs from event source")
    result = subprocess.check_output(
        [
            "git",
            "ls-remote",
            "--exit-code",
            "https://github.com/Knuckles-Team/epistemic-graph.git",
            ref,
            ref + "^{}",
        ],
        text=True,
    )
    verify(result, ref, source)
    print("Canonical release tag still names " + source)


if __name__ == "__main__":
    main()
