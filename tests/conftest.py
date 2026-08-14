import hashlib
import json
import os
import stat
import subprocess
import time

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

TEST_AGENT_ID = "service:test-suite"
TEST_SIGNER_KEY = "test-operation-signer-key"  # sanitizer:ignore — test-only value
TEST_AUDIENCE = "epistemic-graph-test"
TEST_TENANT = "tenant:test"
TEST_POLICY_VERSION = "policy:test"


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
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(
            {TEST_AGENT_ID: TEST_SIGNER_KEY}
        ),
    }
    if persist_dir is not None:
        env["GRAPH_SERVICE_PERSIST_DIR"] = persist_dir
    return env


def find_server_binary() -> str | None:
    """Locate an already-built `full`-featured `epistemic-graph-server`.

    Checks, in order: `$CARGO_TARGET_DIR` (this worktree's own isolated build, per
    the repo's `.cargo/config.toml` -- never a target dir shared with another
    worktree), the repo-relative `target-isolated` that config file defaults to,
    then the legacy `target` layout a plain `cargo build` (with no override at
    all) would use. Never triggers a build itself -- the session fixture (or a
    prior manual build) already paid that cost for the SAME `full` feature set
    every caller of this helper needs. A module that spawns its own server
    subprocess directly (rather than going through the session fixture's own
    `cargo run`, which always lands wherever `CARGO_TARGET_DIR` points) MUST use
    this instead of a hardcoded `target/debug/...` path -- the committed
    `.cargo/config.toml` defaults every build in this repo to `target-isolated`,
    so a hardcoded `target/debug` path never resolves in an ordinary checkout,
    not just a multi-worktree host that also exports the env var.
    """
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    candidates = []
    target_dir_env = os.environ.get("CARGO_TARGET_DIR")
    if target_dir_env:
        candidates.append(os.path.join(target_dir_env, "release", "epistemic-graph-server"))
        candidates.append(os.path.join(target_dir_env, "debug", "epistemic-graph-server"))
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
    digest = str(
        os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY_SHA256", "") or ""
    ).strip()
    if digest:
        hasher = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        if hasher.hexdigest() != digest.lower():
            return None
    return path


@pytest.fixture(scope="session", autouse=True)
def start_epistemic_graph_server(request, tmp_path_factory):
    # Exact-artifact certification owns and restarts its supplied binary, while
    # static contract tests do not need an engine. When every selected test is in
    # either category, never start (or implicitly Cargo-build) the shared engine.
    selected = getattr(request.session, "items", ())
    if selected and all(
        item.get_closest_marker("exact_artifact") is not None
        or item.get_closest_marker("no_engine") is not None
        for item in selected
    ):
        yield None
        return
    rust_dir = os.path.join(os.path.dirname(__file__), "..")
    rust_dir = os.path.abspath(rust_dir)
    runtime_dir = tmp_path_factory.mktemp("epistemic-graph-runtime")
    socket_path = str(runtime_dir / "engine.sock")
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

    process = subprocess.Popen(
        command,
        cwd=rust_dir,
        env={
            **os.environ,
            **server_env,
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # A cold `cargo run` still pays a full relink of this crate's large
    # `full`-feature binary even when nothing needs recompiling (observed
    # ~40s on a loaded build host) -- the previous 15s budget (30 * 0.5s)
    # was tight enough to make the FIRST test of a session flake with a
    # confusing downstream `FileNotFoundError` from the client racing a
    # socket that was still on its way, not a real server-start failure.
    # Generous, bounded budget; still fails fast once the socket is live.
    for _ in range(240):
        if os.path.exists(socket_path):
            break
        time.sleep(0.5)

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

    process.terminate()
    process.wait()
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
