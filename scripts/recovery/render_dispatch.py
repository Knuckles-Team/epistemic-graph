#!/usr/bin/env python3
"""Render a reviewable caller after the reusable workflow has a real commit SHA.

Does not commit, publish, dispatch, or edit GitHub runner policy.
"""

import argparse
import re
from pathlib import Path


def render(sha, template):
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("a reviewed full lowercase commit SHA is required")
    if sha == "0" * 40:
        raise ValueError("a real commit SHA is required")
    return template.replace("REVIEWED_REUSABLE_SHA", sha)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("reviewed_reusable_sha")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    template = (
        Path(__file__).with_name("eg-release-recovery-dispatch.yml.in").read_text()
    )
    with args.output.open("x") as target:
        target.write(render(args.reviewed_reusable_sha, template))
    print("Review and publish the caller separately; no GitHub action was performed.")
