import assert from "node:assert/strict";
import test from "node:test";
import { encode } from "@msgpack/msgpack";

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

function client(options = {}) {
  return new EpistemicGraphThinClient({
    host: "engine.invalid",
    port: 9100,
    authSecret: "test-envelope-secret",
    verifiedContext: context(),
    ...options,
  });
}

function responseFrame(response) {
  const body = encode(response);
  const frame = Buffer.alloc(4 + body.length);
  frame.writeUInt32BE(body.length);
  Buffer.from(body).copy(frame, 4);
  return frame;
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

test("constructor preserves endpoint selection and authority validation order", () => {
  assert.throws(() => new EpistemicGraphThinClient(null), /options are required/);
  assert.throws(
    () => client({ authSecret: "", verifiedContext: null }),
    /authentication secret is required/,
  );
  assert.throws(
    () => client({ verifiedContext: null, port: "9100" }),
    /verifiedContext must be an object/,
  );
  assert.throws(() => client({ port: "9100" }), /TCP port is required/);
  const tcp = client();
  assert.equal(tcp.socketPath, null);
  assert.equal(tcp.host, "engine.invalid");
  assert.equal(tcp.port, 9100);
  const uds = client({ host: null, socketPath: "/configured/engine.sock" });
  assert.equal(uds.socketPath, "/configured/engine.sock");
  assert.equal(uds.host, null);
  assert.equal(Object.isFrozen(uds.verifiedContext), true);
});

function assertEachManagerRoleRejected(connection, registration, roles) {
  for (const role of roles) {
    assert.throws(
      () => connection.registerIdentity({ ...registration, role }),
      /Manager role must contain only subordinates/,
    );
  }
}

test("identity registration preserves the closed Manager role shape", async () => {
  const connection = client();
  connection._send = (_method, params) => Promise.resolve(params);
  const registration = {
    agentId: "worker:test",
    teams: [],
    roles: [],
    signerId: "service:test",
    signerKey: "test-operation-key",
  };
  const params = await connection.registerIdentity({
    ...registration,
    role: { Manager: { subordinates: ["worker:a", "worker:b"] } },
  });
  assert.deepEqual(params.role, {
    Manager: { subordinates: ["worker:a", "worker:b"] },
  });
  assert.match(params.signature, /^service:test:/);
  assertEachManagerRoleRejected(connection, registration, [
    null,
    [],
    {},
    { Manager: null },
    { Manager: { extra: [] } },
  ]);
  assert.throws(
    () => connection.registerIdentity({
      ...registration,
      role: { Manager: { subordinates: ["worker:a", "worker:a"] } },
    }),
    /Manager subordinates contains a duplicate entry/,
  );
});

test("multisig validation preserves error order and sorted signer execution", async () => {
  const connection = client();
  assert.throws(
    () => connection.applyMultisigMutation({ signerKeys: {}, threshold: 1, query: "" }),
    /threshold requires at least/,
  );
  assert.throws(
    () => connection.applyMultisigMutation({
      signerKeys: { "service:a": 7 }, threshold: 1, mutationType: "mutation", query: "",
    }),
    /mutationType and query must be non-empty/,
  );
  assert.throws(
    () => connection.applyMultisigMutation({
      signerKeys: { "service:a": 7 }, threshold: 1, mutationType: "mutation", query: "query",
    }),
    /operation signer ids and keys must be non-empty/,
  );
  connection._send = (method, params, graph, idempotencyKey) =>
    Promise.resolve({ method, params, graph, idempotencyKey });
  const sent = await connection.applyMultisigMutation({
    signerKeys: { "service:z": "secret-z", "service:a": "secret-a" },
    threshold: 2,
    mutationType: "mutation",
    query: "query",
  });
  assert.equal(sent.method, "ApplyMultisigMutation");
  assert.equal(sent.graph, "__commons__");
  assert.match(sent.idempotencyKey, /^operation:sha256:/);
  assert.match(sent.params.signatures[0], /^service:a:/);
  assert.match(sent.params.signatures[1], /^service:z:/);
  assert.equal(JSON.stringify(sent).includes("secret-"), false);
});

function trackPendingIds(connection, settled, ids) {
  for (const id of ids) {
    connection._pending.set(id, {
      resolve: (result) => settled.push([id, result]),
      reject: (error) => settled.push([id, error.message]),
    });
  }
}

test("fragmented coalesced responses preserve correlation, errors and binary results", () => {
  const connection = client();
  connection._sock = { destroy: () => assert.fail("valid responses closed the socket") };
  const settled = [];
  trackPendingIds(connection, settled, [1, 2, 3]);
  const frames = Buffer.concat([
    responseFrame({ id: 99, result: "unknown" }),
    responseFrame({ id: 2, error: 7 }),
    responseFrame({ id: 1, result: encode({ bytes: Uint8Array.from([0, 10, 255]) }) }),
    responseFrame({ id: 3, result: "done" }),
  ]);
  connection._onData(frames.subarray(0, 2));
  assert.equal(connection._pending.size, 3);
  connection._onData(frames.subarray(2, 9));
  assert.equal(connection._pending.size, 3);
  connection._onData(frames.subarray(9));
  assert.ok(settled[1][1].bytes instanceof Uint8Array);
  assert.deepEqual(settled, [
    [2, "7"],
    [1, { bytes: settled[1][1].bytes }],
    [3, "done"],
  ]);
  assert.deepEqual(Array.from(settled[1][1].bytes), [0, 10, 255]);
  assert.equal(connection._pending.size, 0);
  assert.equal(connection._buf.length, 0);
});

function assertInvalidResponseRejectsPending(invalid, expected) {
  const connection = client();
  let destroyed = 0;
  const rejected = [];
  connection._sock = { destroy: () => { destroyed += 1; } };
  connection._pending.set(1, {
    resolve: () => assert.fail("invalid response resolved pending work"),
    reject: (error) => rejected.push(error.message),
  });
  connection._onData(Buffer.concat([invalid, responseFrame({ id: 1, result: "later" })]));
  assert.deepEqual(rejected, [expected]);
  assert.equal(destroyed, 1);
  assert.equal(connection._pending.size, 0);
}

function assertEachInvalidResponseRejectsPending(cases) {
  for (const [invalid, expected] of cases) {
    assertInvalidResponseRejectsPending(invalid, expected);
  }
}

test("invalid responses reject pending work and stop before the next frame", () => {
  assertEachInvalidResponseRejectsPending([
    [Buffer.from([0, 0, 0, 1, 0xc1]), "response was not valid MessagePack"],
    [responseFrame({ id: 1.5, result: "invalid" }), "response is missing its correlation id"],
    [Buffer.alloc(4), "response exceeded the resource limit"],
  ]);
});

test("invalid compact Raw results preserve decoder errors after correlation removal", () => {
  const connection = client();
  connection._sock = { destroy: () => assert.fail("Raw decode errors do not close the socket") };
  connection._pending.set(1, {
    resolve: () => assert.fail("invalid Raw result resolved"),
    reject: () => assert.fail("Raw errors propagate from the decoder"),
  });
  assert.throws(() => connection._onData(responseFrame({ id: 1, result: Uint8Array.of(0xc1) })));
  assert.equal(connection._pending.has(1), false);
});
