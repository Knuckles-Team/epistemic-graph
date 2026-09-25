"""``RepositorySnapshot`` / ``Branch`` / ``Tag`` / ``ChangeEvent`` (U-47, W01).

CONCEPT:EG-KG.ontology.repository-provenance-snapshot

Git identity and history, additive to the existing ``:Commit``/``:Author``/
``:File`` model the source ingestion path already ingests via one
``git log --numstat`` pass (``AUTHORED``/``PARENT``/``TOUCHED``/
``FILE_CHANGES_WITH``). That module already owns the commit DAG; this one
does NOT re-derive it — it anchors four additional, currently-missing node
types onto the SAME ``commit:<sha>`` id convention
(``git_history`` writes those directly, this module never
constructs one) so a classification claim's ``source_snapshot`` field has
a real, queryable node to point at instead of
an opaque string:

* :class:`RepositorySnapshot` — "the state of a repo at one commit", the
  version-control-shaped counterpart to an artifact's ``source_version``.
* :class:`Branch` / :class:`Tag` — named, moving (branch) or fixed (tag)
  pointers at a commit.
* :class:`ChangeEvent` — one repo-scoped historical event (a commit landing,
  a branch moving, a tag being cut) that a classification claim's evidence
  can point at when the claim is really about repository HISTORY rather than
  file content (e.g. "this module's ownership category is derived from who
  authored the majority of its recent commits").

None of the four modify source truth — they are read-only projections of git
metadata already present in the repository, exactly the way ``git_history``
already treats commits as immutable, append-only history.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

__all__ = [
    "REPOSITORY_SNAPSHOT_NODE_TYPE",
    "BRANCH_NODE_TYPE",
    "TAG_NODE_TYPE",
    "CHANGE_EVENT_NODE_TYPE",
    "POINTS_AT",
    "SNAPSHOT_OF",
    "RECORDS",
    "repository_snapshot_id",
    "branch_id",
    "tag_id",
    "change_event_id",
    "RepositorySnapshot",
    "Branch",
    "Tag",
    "ChangeEvent",
]

REPOSITORY_SNAPSHOT_NODE_TYPE = "RepositorySnapshot"
BRANCH_NODE_TYPE = "Branch"
TAG_NODE_TYPE = "Tag"
CHANGE_EVENT_NODE_TYPE = "ChangeEvent"

#: Branch/Tag -> the commit they currently resolve to.
POINTS_AT = "POINTS_AT"
#: RepositorySnapshot -> the commit it was captured at.
SNAPSHOT_OF = "SNAPSHOT_OF"
#: RepositorySnapshot -> a ChangeEvent that occurred within it.
RECORDS = "RECORDS"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def repository_snapshot_id(repo_id: str, commit_sha: str) -> str:
    """Deterministic id — same ``(repo, commit)`` always resolves to the same
    snapshot node, so re-ingesting the same commit upserts, never duplicates."""
    return f"repo_snapshot:{repo_id}:{commit_sha}"


def branch_id(repo_id: str, name: str) -> str:
    return f"branch:{repo_id}:{name}"


def tag_id(repo_id: str, name: str) -> str:
    return f"tag:{repo_id}:{name}"


def change_event_id(repo_id: str, commit_sha: str, kind: str) -> str:
    digest = hashlib.sha256(
        f"{repo_id}\x1f{commit_sha}\x1f{kind}".encode()
    ).hexdigest()[:32]
    return f"change_event:{digest}"


@dataclass(frozen=True)
class RepositorySnapshot:
    """The state of one repository at one commit — the anchor a
    ``ClassificationClaim.source_snapshot`` names for a repo-sourced artifact."""

    repo_id: str
    commit_sha: str
    ref: str = ""
    captured_at: str = field(default_factory=_now_iso)

    @property
    def snapshot_id(self) -> str:
        return repository_snapshot_id(self.repo_id, self.commit_sha)

    def to_node(self) -> dict[str, Any]:
        return {
            "id": self.snapshot_id,
            "node_type": REPOSITORY_SNAPSHOT_NODE_TYPE,
            "repo_id": self.repo_id,
            "commit_sha": self.commit_sha,
            "ref": self.ref,
            "captured_at": self.captured_at,
        }

    def to_graph_slice(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """``(entities, relationships)`` linking to the EXISTING ``commit:<sha>``
        node ``git_history`` already writes — this never constructs a Commit."""
        entities = [self.to_node()]
        relationships = [
            {
                "source": self.snapshot_id,
                "target": f"commit:{self.commit_sha}",
                "relationship": SNAPSHOT_OF,
            }
        ]
        return entities, relationships


@dataclass(frozen=True)
class Branch:
    """A named, moving pointer at a commit."""

    repo_id: str
    name: str
    head_commit_sha: str

    @property
    def branch_node_id(self) -> str:
        return branch_id(self.repo_id, self.name)

    def to_node(self) -> dict[str, Any]:
        return {
            "id": self.branch_node_id,
            "node_type": BRANCH_NODE_TYPE,
            "repo_id": self.repo_id,
            "name": self.name,
            "head_commit_sha": self.head_commit_sha,
        }

    def to_graph_slice(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        entities = [self.to_node()]
        relationships = [
            {
                "source": self.branch_node_id,
                "target": f"commit:{self.head_commit_sha}",
                "relationship": POINTS_AT,
            }
        ]
        return entities, relationships


@dataclass(frozen=True)
class Tag:
    """A named, fixed pointer at a commit."""

    repo_id: str
    name: str
    commit_sha: str
    annotation: str = ""

    @property
    def tag_node_id(self) -> str:
        return tag_id(self.repo_id, self.name)

    def to_node(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "id": self.tag_node_id,
            "node_type": TAG_NODE_TYPE,
            "repo_id": self.repo_id,
            "name": self.name,
            "commit_sha": self.commit_sha,
        }
        if self.annotation:
            row["annotation"] = self.annotation
        return row

    def to_graph_slice(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        entities = [self.to_node()]
        relationships = [
            {
                "source": self.tag_node_id,
                "target": f"commit:{self.commit_sha}",
                "relationship": POINTS_AT,
            }
        ]
        return entities, relationships


@dataclass(frozen=True)
class ChangeEvent:
    """One repo-scoped historical event (a commit landing, a branch move, a
    tag cut) a claim's evidence can cite when the claim concerns repository
    HISTORY rather than file content."""

    repo_id: str
    commit_sha: str
    kind: str  # "commit" | "branch_move" | "tag_cut" | ...
    occurred_at: str
    subject_id: str = ""  # e.g. the file/entity this event concerns, if any

    @property
    def event_id(self) -> str:
        return change_event_id(self.repo_id, self.commit_sha, self.kind)

    def to_node(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "id": self.event_id,
            "node_type": CHANGE_EVENT_NODE_TYPE,
            "repo_id": self.repo_id,
            "commit_sha": self.commit_sha,
            "kind": self.kind,
            "occurred_at": self.occurred_at,
        }
        if self.subject_id:
            row["subject_id"] = self.subject_id
        return row
