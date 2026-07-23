import assert from "node:assert/strict";
import test from "node:test";

import { EpistemicGraphThinClient, validateRequestContext } from "./index.mjs";

function context() {
  return {
    principal: "service:test",
    tenant: "tenant:test",
    audience: "engine:test",
    agent_id: "service:test",
    roles: ["client"],
    scopes: ["graph:read"],
    policy_version: "policy:test",
    delegation: [],
  };
}

test("request context rejects missing and duplicate claims", () => {
  const missing = context();
  delete missing.scopes;
  assert.throws(() => validateRequestContext(missing), /missing required claims/);
  assert.throws(
    () => validateRequestContext({ ...context(), roles: ["client", "client"] }),
    /duplicate/,
  );
});

test("signer emits the current bound envelope", () => {
  const client = new EpistemicGraphThinClient({
    host: "engine.invalid",
    port: 9100,
    authSecret: "test-envelope-secret",
    graph: "graph:test",
    verifiedContext: context(),
  });
  const token = client._sign(
    7,
    "graph:test",
    "DeleteExchange",
    { exchange: "events" },
    "request:test",
  );
  assert.match(token, /^eg2\./);
  const envelope = JSON.parse(Buffer.from(token.slice(4), "hex").toString("utf8"));
  assert.equal(envelope.context.agent_id, "service:test");
  assert.equal(envelope.idempotency_key, "request:test");
  assert.equal(token.includes("test-envelope-secret"), false);
});

test("bootstrap signs without retaining its operation key", async () => {
  const bootstrapContext = {
    ...context(),
    roles: [],
    scopes: ["security:bootstrap"],
  };
  const client = new EpistemicGraphThinClient({
    host: "engine.invalid",
    port: 9100,
    authSecret: "test-envelope-secret",
    verifiedContext: bootstrapContext,
  });
  let captured;
  client._send = (method, params, graph, idempotencyKey) => {
    captured = { method, params, graph, idempotencyKey };
    return Promise.resolve("ok");
  };
  assert.equal(
    await client.bootstrapSystemIdentity({
      agentId: "service:test",
      signerId: "service:test",
      signerKey: "test-operation-key",
    }),
    "ok",
  );
  assert.equal(captured.method, "RegisterIdentity");
  assert.match(captured.params.signature, /^service:test:/);
  assert.equal(JSON.stringify(captured).includes("test-operation-key"), false);
});

test("tag operations carry the current owner and explicit clock", async () => {
  const client = new EpistemicGraphThinClient({
    host: "engine.invalid",
    port: 9100,
    authSecret: "test-envelope-secret",
    verifiedContext: context(),
  });
  const sent = [];
  client._send = (method, params) => {
    sent.push([method, params]);
    return Promise.resolve(true);
  };

  await client.brokerAckTag(7, { consumer: "worker:a" });
  await client.brokerNackTag(8, {
    consumer: "worker:b",
    requeue: true,
    nowMs: 1_000,
  });
  await client.brokerRenewTag(9, {
    consumer: "worker:c",
    nowMs: 1_100,
    leaseMs: 500,
  });

  assert.deepEqual(sent, [
    ["BrokerAckTag", { delivery_tag: 7, consumer: "worker:a" }],
    [
      "BrokerNackTag",
      {
        delivery_tag: 8,
        consumer: "worker:b",
        requeue: true,
        now_ms: 1_000,
      },
    ],
    [
      "BrokerRenewTag",
      {
        delivery_tag: 9,
        consumer: "worker:c",
        now_ms: 1_100,
        lease_ms: 500,
      },
    ],
  ]);
});

test("restore requires the explicit current shard layout", async () => {
  const client = new EpistemicGraphThinClient({
    host: "engine.invalid",
    port: 9100,
    authSecret: "test-envelope-secret",
    verifiedContext: context(),
  });
  let captured;
  client._send = (method, params) => {
    captured = [method, params];
    return Promise.resolve({ restored_shards: 2 });
  };

  await client.restore("scheduled-001", 2);
  assert.deepEqual(captured, [
    "Restore",
    { source: "scheduled-001", target_shards: 2 },
  ]);
  assert.throws(() => client.restore("scheduled-001", 0), /between 1 and 64/);
});
