"""Rust-source guards, re-homed from the deleted `tests/test_protocol_parity.py`.

Two assertions in that file were never about Python/Rust method parity -- they were
text guards over engine source that had been parked there. The parity computation is
now a byproduct of the engine contract, so the file is gone; these two are not
superseded by anything and are carried forward with the SAME predicate over the SAME
sources.

Deliberately NOT widened. Running the `to_vec_named(..).unwrap_or_default()` predicate
over every handler instead of the original two finds three live instances in
`src/server/handlers/knowledge_stream/families.rs` (lines 203, 315, 352), where an
encoding failure silently becomes an empty MessagePack body inside a returned row --
the exact defect this guard exists to forbid, in a file the guard never looked at.
Fixing them means editing `src/server/handlers/**`, which is not this lane's to change,
and pinning the three as "known" would be the ratchet this project bans. They are
reported to the handler-owning lane instead; this file keeps the original scope so it
states something true rather than something amber.

Pure stdlib: it reads Rust sources as text and needs no engine, no cargo, no pytest.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_QUERY_HANDLER = _ROOT / "src" / "server" / "handlers" / "query.rs"
_RDF_HANDLER = _ROOT / "src" / "server" / "handlers" / "rdf.rs"
_REDB_STORE = _ROOT / "src" / "redb_store.rs"
_AMQP_WIRE = _ROOT / "src" / "server" / "amqp_wire" / "mod.rs"
_HOOKS = _ROOT / ".pre-commit-config.yaml"
_RELEASE = _ROOT / ".github" / "workflows" / "release.yml"

# A cargo subcommand that resolves dependencies can REWRITE Cargo.lock; `--locked` is
# what
# stops it. Anything else (`fmt`) cannot.
_RESOLVING_SUBCOMMANDS = ("run", "test", "check", "build", "clippy")


def _rust_sources(*roots: Path) -> str:
    chunks: list[str] = []
    for root in roots:
        if root.is_file():
            chunks.append(root.read_text(encoding="utf-8"))
            continue
        chunks.extend(
            path.read_text(encoding="utf-8") for path in sorted(root.rglob("*.rs"))
        )
    return "\n".join(chunks)


def _cargo_invocations(path: Path) -> list[tuple[int, str]]:
    """`(line number, command)` for every resolving cargo invocation in a gate file."""
    found: list[tuple[int, str]] = []
    for number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if line.lstrip().startswith("#"):
            continue
        for match in re.finditer(r"cargo\s+(\w[\w-]*)", line):
            if match.group(1) in _RESOLVING_SUBCOMMANDS:
                found.append((number, line.strip()))
                break
    return found


class GateInvocationGuards(unittest.TestCase):
    """A `--locked` gate proves nothing if something in the same job may write the lock.

    This lane learned it the expensive way: the engine-contract gate ran
    `cargo run … gen_contract -- --check` with no `--locked`, that unlocked run repaired
    `Cargo.lock` on disk, and the `--locked` step after it then validated cargo's own
    repair and reported clean while four dependency entries were genuinely missing.
    """

    def test_every_engine_contract_gate_invocation_is_locked(self) -> None:
        unlocked = [
            f"{path.name}:{number}: {command}"
            for path in (_HOOKS, _RELEASE)
            for number, command in _cargo_invocations(path)
            if ("gen_contract" in command or "eg-capabilities" in command)
            and "--locked" not in command
        ]
        self.assertEqual(unlocked, [], "\n".join(unlocked))

    def test_the_scan_sees_both_gate_files(self) -> None:
        """A scanner that matches nothing would pass the assertion above vacuously."""
        for path in (_HOOKS, _RELEASE):
            self.assertTrue(path.is_file(), f"{path} is missing")
            self.assertGreater(len(_cargo_invocations(path)), 3, f"{path.name}")

    def test_the_wider_lock_hygiene_gap_is_visible(self) -> None:
        """Report, do not ratchet.

        Most cargo invocations in these two files predate this lane and carry no
        `--locked`; asserting the rule repo-wide here would block the repo on other
        lanes' work, and pinning today's count would be the ratchet this project bans.
        The assertion is scoped to the invocations this lane owns; this test prints the
        rest so the gap is visible rather than silently accepted.
        """
        unlocked = [
            f"{path.name}:{number}: {command}"
            for path in (_HOOKS, _RELEASE)
            for number, command in _cargo_invocations(path)
            if "--locked" not in command
        ]
        if unlocked:
            print(
                "\nlock-hygiene gap (not owned by the engine-contract lane):\n  "
                + "\n  ".join(unlocked)
            )


class RustSourceGuards(unittest.TestCase):
    def test_raw_result_encoding_has_no_empty_or_panic_fallback(self) -> None:
        """Every typed Raw response propagates serializer failures.

        `unwrap_or_default` turns an encoding error into a SUCCESSFUL empty byte string
        and `expect` turns the same boundary failure into a process panic. Both break
        the
        one fallible Raw contract: the caller must see the error.
        """
        handlers = _rust_sources(_QUERY_HANDLER, _RDF_HANDLER)
        self.assertIsNone(
            re.search(
                r"rmp_serde::to_vec_named\([^;]*?\)\.unwrap_or_default\(\)",
                handlers,
                re.DOTALL,
            ),
            "a handler silently encodes an empty Raw body on serializer failure",
        )
        store = _REDB_STORE.read_text(encoding="utf-8")
        self.assertIsNone(
            re.search(
                r"ResultPayload::raw\([^;]+?\)\s*\.(?:expect|unwrap)\(",
                store,
                re.DOTALL,
            ),
            "redb_store panics instead of returning a Raw encoding error",
        )

    def test_amqp_connection_spawn_is_not_unwrapped(self) -> None:
        """`tokio::spawn` returns a JoinHandle directly, never a Result."""
        source = _AMQP_WIRE.read_text(encoding="utf-8")
        serve = source[
            source.index("pub async fn serve(") : source.index("async fn claim_one(")
        ]
        self.assertIsNone(
            re.search(r"tokio::spawn\([\s\S]*?\)\s*\.unwrap\(\)", serve),
            "tokio::spawn is unwrapped in the AMQP serve loop",
        )

    def test_the_guarded_sources_still_exist(self) -> None:
        """A guard that silently reads nothing is a guard that cannot fail."""
        for path in (_QUERY_HANDLER, _RDF_HANDLER, _REDB_STORE, _AMQP_WIRE):
            self.assertTrue(path.is_file(), f"{path} is missing")
            self.assertGreater(len(path.read_text(encoding="utf-8")), 1000)


if __name__ == "__main__":
    unittest.main()
