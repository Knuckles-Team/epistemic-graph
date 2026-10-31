"""Immutable run admission recovery over the authenticated native TCP wire."""

import copy
import time
import uuid

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_AUDIENCE,
    TEST_POLICY_VERSION,
    TEST_SIGNER_KEY,
    TEST_TENANT,
    bootstrap_context,
    request_context,
    strict_server_env,
)
from test_work_item_retry_identity import _submit_request

from medulla.client import EngineResponseError, SyncMedullaClient

pytestmark = [pytest.mark.no_engine, pytest.mark.spec("MEDULLA-DURABLE-KERNEL-R028.4.1")]


@pytest.fixture
def authenticated_plan_client(tmp_path):
    from test_native_no_auth_served import server

    # Public loopback fixture value, matching the authenticated TaskPlan suite.
    test_auth_secret = "-".join(("test", "epistemic", "graph", "secret"))
    env = strict_server_env(
        str(tmp_path / "security"),
        auth_secret=test_auth_secret,
        persist_dir=str(tmp_path / "data"),
    )
    env["MEDULLA_ENCRYPTION_KEY"] = "test-medulla-encryption-key"
    with server(tmp_path, mode="authenticated", extra_env=env) as address:
        tcp = f"{address[0]}:{address[1]}"
        bootstrap = SyncMedullaClient.connect(
            tcp_addr=tcp,
            auth_secret=test_auth_secret,
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
        yield SyncMedullaClient.connect(
            tcp_addr=tcp,
            auth_secret=test_auth_secret,
            verified_context=request_context(),
        )


def test_authenticated_run_recovery_preserves_exact_admission(request):
    client = request.getfixturevalue("authenticated_plan_client")
    run_id = "recovery-" + uuid.uuid4().hex
    now = int(time.time() * 1000)
    original = _submit_request()
    original.update(
        run_id=run_id,
        work_item_id=None,
        idempotency_key=run_id,
        metadata={"premises": [{"class": "observed"}]},
    )
    original["context"].update(
        request_id=run_id,
        trace_id=run_id,
        subject_id=TEST_AGENT_ID,
        tenant_id=TEST_TENANT,
        agent_id=TEST_AGENT_ID,
        audience=TEST_AUDIENCE,
        policy_version=TEST_POLICY_VERSION,
        graph="__commons__",
        scopes=["*"],
        issued_at_ms=now,
        expires_at_ms=now + 60_000,
    )
    try:
        assert client.work_items.get_run(tenant=TEST_TENANT, run_id=run_id) is None
        submitted = client.work_items.submit(original)
        refreshed = copy.deepcopy(original)
        refreshed["context"]["issued_at_ms"] += 1
        refreshed["context"]["expires_at_ms"] += 1
        with pytest.raises(EngineResponseError, match="IDEMPOTENCY_CONFLICT"):
            client.work_items.submit(refreshed)
        recovered = client.work_items.get_run(tenant=TEST_TENANT, run_id=run_id)
        assert recovered["admission"] == original
        assert recovered["work_item"]["work_item_id"] == submitted["work_item_id"]
        plain = client.work_items.get(
            tenant=TEST_TENANT, work_item_id=submitted["work_item_id"]
        )
        assert "admission" not in plain and "run_admission_request" not in plain
        client.work_items.cas_metadata(
            tenant=TEST_TENANT,
            work_item_id=submitted["work_item_id"],
            expected_status=["ready"],
            expected_metadata=original["metadata"],
            set_metadata={"changed": True},
            now_ms=now,
            idempotency_key=run_id + ":metadata",
        )
        client.work_items.cancel(
            tenant=TEST_TENANT,
            work_item_id=submitted["work_item_id"],
            idempotency_key=run_id + ":cancel",
            now_ms=now,
        )
        terminal = client.work_items.get_run(tenant=TEST_TENANT, run_id=run_id)
        assert terminal["admission"] == original
        assert terminal["work_item"]["metadata"] == {"changed": True}
        assert terminal["work_item"]["status"] == "cancelled"
    finally:
        client.close()
