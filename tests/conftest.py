import hashlib
import importlib.util
import json
import os
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

TEST_AGENT_ID = "service:test-suite"
TEST_SIGNER_KEY = "test-operation-signer-key"  # sanitizer:ignore — test-only value
TEST_AUDIENCE = "epistemic-graph-test"
TEST_TENANT = "tenant:test"
TEST_POLICY_VERSION = "policy:test"

# NE-247. NE-065 replaced the flat `{signer_id: key}` signer registry with a
# SCOPED one and made the flat shape fail closed (`allowed_roles: []`,
# `may_grant_system: false`) rather than silently unlimited -- see
# `SignerKeySpec::Legacy`'s justification in `src/server/auth.rs`. This suite's
# registry was never migrated, so `authorize_grant` denied the session
# fixture's own `bootstrap_system_identity` (an `AgentRole::System` grant) and
# 534 of 548 tests errored at setup with `signer is not authorized for this
# identity registration`. The registry is now scoped explicitly.
#
# `may_grant_system` is genuinely required here: the fixture registers the
# suite's own genesis System identity, which is exactly the one grant that flag
# exists to authorise. `authorize_grant` still holds the rest of NE-065's shape
# on this path -- a System grant must carry NO RBAC roles and must be a SELF
# registration (`agent_id == signer`), both of which the fixture satisfies.
#
# `TEST_SIGNER_ALLOWED_ROLES` is an EXPLICIT enumeration, not a wildcard: a
# bare `"*"` is rejected by `RoleAllowance::parse` by design, and a
# `"namespace:*"` prefix would not match these bare role names anyway. Any test
# that registers an identity with a new RBAC role name must add it here -- that
# is the scoping working, not friction to route around.
TEST_SIGNER_ALLOWED_ROLES = [
    "commons-access",
    "worker1-access",
    "flip-reader",
    "lease-writer",
    "reports-reader",
]


#: A node -> edge -> commit write as real, engine-decodable requests. Mock
#: transports use it where they need a named sequence: the client signs the
#: body the engine re-derives, so it refuses an undecodable request locally.
WRITE_SEQUENCE: tuple[tuple[str, dict[str, object]], ...] = (
    ("AddNode", {"node_id": "n1", "properties_msgpack": b"\x80"}),
    ("AddEdge", {"source_id": "n1", "target_id": "n2", "properties_msgpack": b"\x80"}),
    ("Commit", {"txn_id": "t1", "idempotency_key": None}),
)


def request_context(
    *,
    agent_id: str = TEST_AGENT_ID,
    principal: str | None = None,
    roles: list[str] | None = None,
    scopes: list[str] | None = None,
) -> dict[str, object]:
    """Return explicit, non-personal test authority claims."""

    subject = principal or agent_id
    return {
        "principal": subject,
        "tenant": TEST_TENANT,
        "audience": TEST_AUDIENCE,
        "agent_id": agent_id,
        "roles": list(roles if roles is not None else ["test"]),
        "scopes": list(scopes if scopes is not None else ["*"]),
        "policy_version": TEST_POLICY_VERSION,
        "delegation": [] if subject == agent_id else [subject, agent_id],
    }


def bootstrap_context() -> dict[str, object]:
    return request_context(roles=[], scopes=["security:bootstrap"])


def strict_server_env(
    state_dir: str, *, auth_secret: str, persist_dir: str | None = None
) -> dict[str, str]:
    """`persist_dir` is OPTIONAL and keyword-only, defaulting to unset, so every
    existing caller of this helper keeps its exact current behavior.

    `main.rs` refuses to start without a durable-state directory (`error: the
    served engine requires an externally configured durable-state
    directory`) — `start_epistemic_graph_server` below was written before
    that gate existed and never grew a `--persist-dir`/`GRAPH_SERVICE_
    PERSIST_DIR` of its own, so the shared session-scoped engine silently
    failed to bind its socket and every test in the session hung on a
    `FileNotFoundError` racing a server that never started, with the real
    cause sitting unread in the subprocess's captured stderr. Fixed at that
    ONE call site (which now passes `persist_dir` explicitly); every OTHER
    caller of this helper is unchanged.
    """
    env = {
        "GRAPH_SERVICE_AUTH_SECRET": auth_secret,
        # Explicit, deliberate opt-out of the MANDATORY-OIDC posture (secure
        # by default since 2026-07-22 — see auth.rs's `require_oidc()`). This
        # test suite deliberately exercises the plain `eg2.` HMAC-envelope
        # protocol (no Keycloak/OIDC provider is available in CI), which is
        # exactly the documented local/dev use case for this opt-out. A
        # dedicated end-to-end proof that the REAL default (this var unset)
        # refuses to start, and that this opt-out genuinely restores the
        # legacy behavior, lives in test_auth_enforcement.py.
        "EPISTEMIC_GRAPH_REQUIRE_OIDC": "false",
        "EPISTEMIC_GRAPH_AUDIENCE": TEST_AUDIENCE,
        "EPISTEMIC_GRAPH_TENANT": TEST_TENANT,
        "EPISTEMIC_GRAPH_POLICY_VERSION": TEST_POLICY_VERSION,
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR": state_dir,
        # NE-247: the SCOPED registry shape. See TEST_SIGNER_ALLOWED_ROLES above.
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(
            {
                TEST_AGENT_ID: {
                    "key": TEST_SIGNER_KEY,
                    "allowed_roles": TEST_SIGNER_ALLOWED_ROLES,
                    "may_grant_system": True,
                }
            }
        ),
    }
    if persist_dir is not None:
        env["GRAPH_SERVICE_PERSIST_DIR"] = persist_dir
    return env


def find_server_binary() -> str | None:
    """Locate an already-built `full`-featured `epistemic-graph-server`.

    A validated `EPISTEMIC_GRAPH_TEST_BINARY` (see `_prebuilt_test_binary`)
    wins outright. Otherwise checks, in order: `$CARGO_TARGET_DIR` (this
    worktree's own isolated build, per the repo's `.cargo/config.toml` -- never
    a target dir shared with another worktree), the repo-relative
    `target-isolated` that config file defaults to, then the legacy `target`
    layout a plain `cargo build` (with no override at all) would use. Never
    triggers a build itself -- the session fixture (or a prior manual build)
    already paid that cost for the SAME `full` feature set every caller of this
    helper needs. A module that spawns its own server subprocess directly
    (rather than going through the session fixture's own `cargo run`, which
    always lands wherever `CARGO_TARGET_DIR` points) MUST use this instead of a
    hardcoded `target/debug/...` path -- the committed `.cargo/config.toml`
    defaults every build in this repo to `target-isolated`, so a hardcoded
    `target/debug` path never resolves in an ordinary checkout, not just a
    multi-worktree host that also exports the env var.
    """
    prebuilt = _prebuilt_test_binary()
    if prebuilt is not None:
        # The binary the session engine runs is the one every dedicated server
        # runs too: a `release/` candidate below can be any other build that
        # happens to share the target dir.
        return prebuilt
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    candidates = []
    target_dir_env = os.environ.get("CARGO_TARGET_DIR")
    if target_dir_env:
        candidates.append(
            os.path.join(target_dir_env, "release", "epistemic-graph-server")
        )
        candidates.append(
            os.path.join(target_dir_env, "debug", "epistemic-graph-server")
        )
    candidates += [
        os.path.join(root, "target-isolated", "release", "epistemic-graph-server"),
        os.path.join(root, "target-isolated", "debug", "epistemic-graph-server"),
        os.path.join(root, "target", "release", "epistemic-graph-server"),
        os.path.join(root, "target", "debug", "epistemic-graph-server"),
    ]
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate
    return None


def _prebuilt_test_binary() -> str | None:
    """Return a validated `EPISTEMIC_GRAPH_TEST_BINARY` path, or ``None``.

    BUG-045: the pytest pre-push gate previously ALWAYS paid for a `cargo build` +
    `cargo run` of the full-featured engine here, on top of the separate ~41-minute
    PEP 517 `maturin`/`full,ast-extended`/`-C lto=thin -C codegen-units=1` release
    build `uv run --all-extras` triggers just to install this package (a build whose
    OUTPUT this fixture never even uses — it always built and ran its own copy). A
    caller that already has a matching binary (a prior `cargo build --features full`
    in this same checkout, or a CI artifact) can point `EPISTEMIC_GRAPH_TEST_BINARY`
    at it to skip the Cargo build+run entirely. Optional
    `EPISTEMIC_GRAPH_TEST_BINARY_SHA256` verifies its integrity the same way the
    exact-artifact certification tests already do (`test_durable_crash.py`,
    `test_exact_release_campaigns.py`) — unset ⇒ no digest check, just an
    executable-file check. Unset/invalid/missing ⇒ ``None`` and the caller falls
    back to today's cargo build+run, unchanged.
    """

    binary = str(os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY", "") or "").strip()
    if not binary:
        return None
    path = os.path.abspath(binary)
    if not os.path.isfile(path):
        return None
    mode = os.stat(path).st_mode
    if not mode & stat.S_IXUSR:
        return None
    digest = str(os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY_SHA256", "") or "").strip()
    if digest:
        hasher = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        if hasher.hexdigest() != digest.lower():
            return None
    return path


#: Ceiling, in seconds, on a spawned engine reaching a state: accepting
#: connections after start, or exiting after SIGTERM. The fast path returns as
#: soon as the state is reached (tens of milliseconds on an idle host), and a
#: server that exits early fails at once with its own output, so the ceiling is
#: only ever paid by a genuinely stuck server. It is generous because the suite
#: runs a debug `full` binary under xdist on shared build hosts, where a fixed
#: 5 s budget failed healthy servers at a load average of ~40.
SERVER_TIMEOUT_ENV = "EPISTEMIC_GRAPH_TEST_SERVER_TIMEOUT"
_DEFAULT_SERVER_TIMEOUT = 120.0
_OUTPUT_TAIL_BYTES = 8192


def server_timeout() -> float:
    """The start/stop ceiling for a spawned engine (`$SERVER_TIMEOUT_ENV`)."""

    raw = os.environ.get(SERVER_TIMEOUT_ENV, "").strip()
    return float(raw) if raw else _DEFAULT_SERVER_TIMEOUT


def spawn_server(
    command: list[str], *, env: dict[str, str], log_path: Path, cwd: str | None = None
) -> subprocess.Popen:
    """Start an engine with its output in `log_path`, never an unread pipe.

    A long-lived server writing into a pipe nobody drains blocks once the pipe
    buffer fills; the file keeps the output for the failure messages below.
    """

    with open(log_path, "wb") as log:
        return subprocess.Popen(command, cwd=cwd, env=env, stdout=log, stderr=log)


def _socket_accepts(socket_path: str) -> bool:
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(socket_path)
    except OSError:
        return False
    finally:
        probe.close()
    return True


def _server_output(process: subprocess.Popen, log_path: Path | None) -> str:
    """What a stopped server wrote: its log tail, or its captured pipes."""

    if log_path is not None:
        data = log_path.read_bytes() if log_path.exists() else b""
        return f"output ({log_path}):\n" + data[-_OUTPUT_TAIL_BYTES:].decode(
            "utf-8", "replace"
        )
    try:
        out, err = process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        return "output unavailable: the server did not exit after SIGKILL"
    return f"stdout={out!r}\nstderr={err!r}"


def wait_for_server(
    process: subprocess.Popen,
    socket_path: str,
    *,
    name: str,
    log_path: Path | None = None,
) -> None:
    """Block until the engine at `socket_path` accepts a connection.

    Readiness is a successful `connect()`, not the socket file existing (the
    file appears at `bind`, before `listen`). Fails the test with the server's
    own output if it exits first or the `server_timeout()` ceiling passes.
    """

    timeout = server_timeout()
    deadline = time.monotonic() + timeout
    while not _socket_accepts(socket_path):
        if process.poll() is not None:
            pytest.fail(
                f"{name} exited with code {process.returncode} before accepting "
                f"connections on {socket_path}\n{_server_output(process, log_path)}"
            )
        if time.monotonic() >= deadline:
            process.kill()
            process.wait()
            pytest.fail(
                f"{name} did not accept connections on {socket_path} within "
                f"{timeout:g}s (${SERVER_TIMEOUT_ENV} raises the ceiling)\n"
                f"{_server_output(process, log_path)}"
            )
        time.sleep(0.05)


def stop_server(
    process: subprocess.Popen, *, name: str, log_path: Path | None = None
) -> None:
    """SIGTERM, then a bounded wait; a server that ignores it is killed and
    reported rather than hanging the session until pytest-timeout fires."""

    if process.poll() is not None:
        return
    timeout = server_timeout()
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        pytest.fail(
            f"{name} did not exit within {timeout:g}s of SIGTERM\n"
            f"{_server_output(process, log_path)}"
        )


def selection_needs_engine(items) -> bool:
    """Whether a test selection needs the shared session engine.

    Exact-artifact certification owns and restarts its supplied binary, and
    static contract tests (`no_engine`) need no engine at all.
    """

    return not items or not all(
        item.get_closest_marker("exact_artifact") is not None
        or item.get_closest_marker("no_engine") is not None
        for item in items
    )


@pytest.fixture(scope="session", autouse=True)
def start_epistemic_graph_server(request, tmp_path_factory):
    # When no selected test needs the shared engine (see
    # `selection_needs_engine`: exact_artifact / no_engine only), never
    # start (or implicitly Cargo-build) the shared engine.
    if not selection_needs_engine(getattr(request.session, "items", ())):
        yield None
        return
    rust_dir = os.path.join(os.path.dirname(__file__), "..")
    rust_dir = os.path.abspath(rust_dir)
    runtime_dir = tmp_path_factory.mktemp("epistemic-graph-runtime")
    socket_path = str(runtime_dir / "engine.sock")
    log_path = runtime_dir / "engine.log"
    state_dir = str(runtime_dir / "security")
    persist_dir = str(runtime_dir / "persist")
    os.makedirs(persist_dir, exist_ok=True)
    # Real secret so the suite exercises the HMAC auth path end-to-end
    # (an empty secret makes the server refuse to start by design).
    auth_secret = "test-epistemic-graph-secret"
    # Both branches independently found the same defect: `main.rs` exits(2)
    # before opening the listener when no durable-state dir is configured, so the
    # shared session engine silently never started and every dependent test failed
    # at fixture setup. Taking the parameterized form: it is keyword-only and
    # defaults to unset (so every other caller is byte-identical), and unlike the
    # alternative it also CREATES the directory rather than only naming it.
    server_env = strict_server_env(
        state_dir, auth_secret=auth_secret, persist_dir=persist_dir
    )
    # Obviously-non-secret test key material (see `src/crypto.rs`'s
    # `ENCRYPTION_KEY_ENV` doc: "any length; hashed to 32 bytes"). Configuring a
    # `persist_dir` above turns on durable multi-op transactions, which refuse to
    # start without this key (`server/handlers/txn/receipts.rs`); this is the
    # ONLY `strict_server_env` call site that passes `persist_dir`, so no other
    # caller is affected. Respects a value a caller already exported instead of
    # clobbering it.
    server_env.setdefault(
        "EPISTEMIC_GRAPH_ENCRYPTION_KEY",
        os.environ.get(
            "EPISTEMIC_GRAPH_ENCRYPTION_KEY", "test-epistemic-graph-encryption-key"
        ),
    )

    if os.path.exists(socket_path):
        os.remove(socket_path)

    print("Starting epistemic-graph-server...")
    prebuilt = _prebuilt_test_binary()
    if prebuilt is not None:
        print(f"Using pre-built EPISTEMIC_GRAPH_TEST_BINARY: {prebuilt}")
        command = [prebuilt, "--socket-path", socket_path]
    else:
        # Build with `full` (= compute + server): the suite exercises the finance,
        # datascience, reasoning AND ast (ParseFiles/IndexRepository) domains, which
        # a `server`-only build compiles out — every such test would otherwise fail
        # with "Method not available in this server build".
        subprocess.run(
            ["cargo", "build", "--features", "full"], cwd=rust_dir, check=False
        )
        command = [
            "cargo",
            "run",
            "--features",
            "full",
            "--bin",
            "epistemic-graph-server",
            "--",
            "--socket-path",
            socket_path,
        ]

    process = spawn_server(
        command,
        cwd=rust_dir,
        env={
            **os.environ,
            **server_env,
        },
        log_path=log_path,
    )
    # A cold `cargo run` still pays a full relink of this crate's large
    # `full`-feature binary even when nothing needs recompiling (observed
    # ~40s on a loaded build host), which `server_timeout()` covers.
    wait_for_server(process, socket_path, name="session engine", log_path=log_path)

    os.environ["GRAPH_SERVICE_SOCKET"] = socket_path
    os.environ.update(server_env)

    bootstrap = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=auth_secret,
        verified_context=bootstrap_context(),
    )
    try:
        bootstrap.consensus.bootstrap_system_identity(
            agent_id=TEST_AGENT_ID,
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
    finally:
        bootstrap.close()

    yield process

    stop_server(process, name="session engine", log_path=log_path)
    if os.path.exists(socket_path):
        os.remove(socket_path)


@pytest.fixture
def clean_graph():
    """Returns a clean EpistemicGraphClient instance for each test case."""
    socket_path = os.environ.get("GRAPH_SERVICE_SOCKET")
    assert socket_path is not None
    client = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(),
    )
    client.graph.clear()
    return client


@pytest.fixture
def load_script(monkeypatch):
    """Load ``scripts/<name>.py`` as a fresh module for one test.

    The module is registered in ``sys.modules`` under ``name`` (dataclasses and
    ``multiprocessing`` pickling resolve it there) only for the test's
    duration; ``monkeypatch`` removes or restores the entry afterwards, so no
    loaded module or its monkeypatched state leaks into another test.
    """

    def load(name: str):
        path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    return load
