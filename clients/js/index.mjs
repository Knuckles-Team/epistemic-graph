// CONCEPT:EG-328 — Thin JS/Node client for the Program-B engine Methods (B1.7).
//
// This is a DELIBERATELY THIN binding, NOT a full SDK. It covers ONLY the new wire
// `Method`s that had no client surface — the broker + append-log streams
// (EG-275..284/314), RBAC admin (EG-092), online backup/restore (EG-090), and
// NL->query (EG-080) — over the SAME framed-MessagePack transport the Python client
// uses (4-byte big-endian length prefix + a msgpack `{id, graph, auth_token, method,
// params}` request; responses demuxed by `id`; current signed request-context
// envelopes bind canonical method bodies and replay controls).
//
// The method surface below is GENERATED-FROM-THE-Method-LIST: each function maps 1:1
// to a Rust `Method` variant in crates/eg-types/src/protocol.rs (the exact param
// field names the serde-tagged enum destructures). The full graph/vector/RDF/SQL API
// is intentionally NOT re-implemented here — use the Python client for that.
//
// Deps (Pi-contract: thin): `@msgpack/msgpack` (pure-JS) for framing; Node built-ins
// `net` (UDS/TCP) + `crypto` (HMAC). No native addon, no heavy SDK.

import net from "node:net";
import crypto from "node:crypto";
import { encode, decode } from "@msgpack/msgpack";

const CONTEXT_FIELDS = new Set([
  "principal",
  "tenant",
  "audience",
  "agent_id",
  "roles",
  "scopes",
  "policy_version",
  "delegation",
]);
const MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

export function validateRequestContext(context) {
  if (context === null || typeof context !== "object" || Array.isArray(context)) {
    throw new TypeError("verifiedContext must be an object");
  }
  const fields = Object.keys(context);
  const missing = [...CONTEXT_FIELDS].filter((field) => !(field in context));
  if (missing.length) {
    throw new Error(`verifiedContext is missing required claims: ${missing.join(", ")}`);
  }
  const unsupported = fields.filter((field) => !CONTEXT_FIELDS.has(field));
  if (unsupported.length) {
    throw new Error(`verifiedContext contains unsupported claims: ${unsupported.join(", ")}`);
  }
  for (const field of ["principal", "tenant", "audience", "agent_id", "policy_version"]) {
    if (typeof context[field] !== "string" || !context[field].trim()) {
      throw new Error(`verifiedContext.${field} must be a non-empty string`);
    }
  }
  for (const field of ["roles", "scopes", "delegation"]) {
    if (!Array.isArray(context[field])) {
      throw new TypeError(`verifiedContext.${field} must be an array of strings`);
    }
    const seen = new Set();
    for (const claim of context[field]) {
      if (typeof claim !== "string" || !claim.trim()) {
        throw new Error(`verifiedContext.${field} entries must be non-empty strings`);
      }
      if (seen.has(claim)) {
        throw new Error(`verifiedContext.${field} contains a duplicate entry`);
      }
      seen.add(claim);
    }
  }
  const value = {
    principal: context.principal,
    tenant: context.tenant,
    audience: context.audience,
    agent_id: context.agent_id,
    roles: Object.freeze([...context.roles]),
    scopes: Object.freeze([...context.scopes]),
    policy_version: context.policy_version,
    delegation: Object.freeze([...context.delegation]),
  };
  if (value.principal === value.agent_id) {
    if (value.delegation.length) {
      throw new Error("delegation must be empty when principal is the agent");
    }
  } else if (
    value.delegation.length < 2 ||
    value.delegation[0] !== value.principal ||
    value.delegation.at(-1) !== value.agent_id
  ) {
    throw new Error("delegation must run from principal to effective agent");
  }
  return Object.freeze(value);
}

function appendText(parts, value) {
  const bytes = Buffer.from(value, "utf8");
  const size = Buffer.allocUnsafe(4);
  size.writeUInt32BE(bytes.length);
  parts.push(size, bytes);
}

function appendList(parts, values) {
  const size = Buffer.allocUnsafe(4);
  size.writeUInt32BE(values.length);
  parts.push(size);
  for (const value of values) appendText(parts, value);
}

function appendOperationBytes(parts, value) {
  const bytes = Buffer.from(value);
  const size = Buffer.allocUnsafe(8);
  size.writeBigUInt64BE(BigInt(bytes.length));
  parts.push(size, bytes);
}

function appendOperationList(parts, values) {
  const count = Buffer.allocUnsafe(8);
  count.writeBigUInt64BE(BigInt(values.length));
  appendOperationBytes(parts, count);
  for (const value of values) appendOperationBytes(parts, Buffer.from(value, "utf8"));
}

function validateExplicitStringList(name, values) {
  if (!Array.isArray(values)) {
    throw new TypeError(`${name} must be explicitly supplied as an array`);
  }
  const seen = new Set();
  return values.map((value) => {
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(`${name} entries must be non-empty strings`);
    }
    if (seen.has(value)) throw new Error(`${name} contains a duplicate entry`);
    seen.add(value);
    return value;
  });
}

function normalizeAgentRole(role) {
  if (typeof role === "string") {
    if (role !== "System" && role !== "Agent") {
      throw new Error("role must be System, Agent, or a Manager value");
    }
    return role;
  }
  if (
    role === null ||
    typeof role !== "object" ||
    Array.isArray(role) ||
    Object.keys(role).length !== 1 ||
    role.Manager === null ||
    typeof role.Manager !== "object" ||
    Array.isArray(role.Manager) ||
    Object.keys(role.Manager).length !== 1 ||
    !("subordinates" in role.Manager)
  ) {
    throw new Error("Manager role must contain only subordinates");
  }
  return {
    Manager: {
      subordinates: validateExplicitStringList(
        "Manager subordinates",
        role.Manager.subordinates,
      ),
    },
  };
}

export class EpistemicGraphThinClient {
  /**
   * @param {object} opts
   * @param {string} [opts.socketPath] Configured Unix domain socket path.
   * @param {string} [opts.host] TCP host (alternative to socketPath).
   * @param {number} [opts.port] TCP port.
   * @param {string} [opts.authSecret] HMAC secret (default GRAPH_SERVICE_AUTH_SECRET).
   * @param {string} [opts.graph] Default graph name for requests.
   * @param {object} opts.verifiedContext Complete current authority context.
   */
  constructor(opts) {
    if (opts === null || typeof opts !== "object" || Array.isArray(opts)) {
      throw new TypeError("client options are required");
    }
    this.socketPath =
      opts.socketPath ||
      (opts.host ? null : process.env.GRAPH_SERVICE_SOCKET || null);
    this.host = opts.host || null;
    this.port = opts.port || null;
    this.authSecret = opts.authSecret ?? process.env.GRAPH_SERVICE_AUTH_SECRET ?? "";
    if (typeof this.authSecret !== "string" || !this.authSecret) {
      throw new Error("a non-empty authentication secret is required");
    }
    this.verifiedContext = validateRequestContext(opts.verifiedContext);
    if (!this.socketPath && !this.host) {
      throw new Error("a configured socketPath or TCP host is required");
    }
    if (this.host && !Number.isInteger(this.port)) {
      throw new Error("a TCP port is required when host is configured");
    }
    this.graph = opts.graph || "__commons__";
    this._sock = null;
    this._id = 0;
    this._pending = new Map();
    this._buf = Buffer.alloc(0);
  }

  connect() {
    return new Promise((resolve, reject) => {
      const onConnect = () => {
        this._sock.removeListener("error", reject);
        resolve(this);
      };
      this._sock = this.host
        ? net.createConnection({ host: this.host, port: this.port }, onConnect)
        : net.createConnection({ path: this.socketPath }, onConnect);
      this._sock.once("error", reject);
      this._sock.on("data", (chunk) => this._onData(chunk));
      this._sock.on("close", () => {
        this._rejectPending("connection closed");
      });
    });
  }

  _onData(chunk) {
    this._buf = Buffer.concat([this._buf, chunk]);
    // Frames: 4-byte big-endian length prefix + msgpack body.
    while (this._buf.length >= 4) {
      const len = this._buf.readUInt32BE(0);
      if (len === 0 || len > MAX_RESPONSE_BYTES) {
        this._rejectPending("response exceeded the resource limit");
        this._sock.destroy();
        return;
      }
      if (this._buf.length < 4 + len) break;
      const body = this._buf.subarray(4, 4 + len);
      this._buf = this._buf.subarray(4 + len);
      let resp;
      try {
        resp = decode(body);
      } catch {
        this._rejectPending("response was not valid MessagePack");
        this._sock.destroy();
        return;
      }
      if (resp === null || typeof resp !== "object" || !Number.isSafeInteger(resp.id)) {
        this._rejectPending("response is missing its correlation id");
        this._sock.destroy();
        return;
      }
      const p = this._pending.get(resp.id);
      if (!p) continue;
      this._pending.delete(resp.id);
      if (resp.error != null) {
        p.reject(new Error(String(resp.error)));
      } else {
        let result = resp.result;
        // Compact encoding: a top-level msgpack `bin` is a second Raw layer.
        if (result instanceof Uint8Array) result = decode(result);
        p.resolve(result);
      }
    }
  }

  _rejectPending(message) {
    for (const { reject } of this._pending.values()) reject(new Error(message));
    this._pending.clear();
  }

  _sign(id, graph, method, params, explicitIdempotencyKey = "") {
    const body = encode({ method, params });
    const bodyHash = crypto.createHash("sha256").update(body).digest("hex");
    const idempotencyKey =
      explicitIdempotencyKey ||
      `rpc:sha256:${crypto
        .createHash("sha256")
        .update(`${id}\0${graph}\0${method}\0${bodyHash}`)
        .digest("hex")}`;
    const timestamp = Math.floor(Date.now() / 1000);
    const nonce = crypto.randomBytes(24).toString("hex");
    const parts = [];
    appendText(parts, "eg-envelope-v2");
    const requestId = Buffer.allocUnsafe(8);
    requestId.writeBigUInt64BE(BigInt(id));
    parts.push(requestId);
    for (const value of [
      graph,
      method,
      bodyHash,
      this.verifiedContext.principal,
      this.verifiedContext.tenant,
      this.verifiedContext.audience,
      this.verifiedContext.agent_id,
    ]) {
      appendText(parts, value);
    }
    appendList(parts, this.verifiedContext.roles);
    appendList(parts, this.verifiedContext.scopes);
    appendText(parts, this.verifiedContext.policy_version);
    appendList(parts, this.verifiedContext.delegation);
    const timestampBytes = Buffer.allocUnsafe(8);
    timestampBytes.writeBigUInt64BE(BigInt(timestamp));
    parts.push(timestampBytes);
    appendText(parts, nonce);
    appendText(parts, idempotencyKey);
    const mac = crypto
      .createHmac("sha256", this.authSecret)
      .update(Buffer.concat(parts))
      .digest("hex");
    const envelope = {
      context: this.verifiedContext,
      timestamp,
      nonce,
      idempotency_key: idempotencyKey,
      mac,
    };
    return `eg2.${Buffer.from(JSON.stringify(envelope), "utf8").toString("hex")}`;
  }

  /** Send one current-envelope request and await its correlated response. */
  _send(method, params, graph, idempotencyKey = "") {
    const id = ++this._id;
    const targetGraph = graph || this.graph;
    const req = {
      id,
      graph: targetGraph,
      auth_token: this._sign(id, targetGraph, method, params, idempotencyKey),
      agent_id: this.verifiedContext.agent_id,
      method,
      params,
    };
    const payload = encode(req);
    const frame = Buffer.alloc(4 + payload.length);
    frame.writeUInt32BE(payload.length, 0);
    Buffer.from(payload).copy(frame, 4);
    return new Promise((resolve, reject) => {
      this._pending.set(id, { resolve, reject });
      this._sock.write(frame);
    });
  }

  _newOperationIdempotencyKey() {
    return `operation:sha256:${crypto
      .createHash("sha256")
      .update(crypto.randomBytes(32))
      .digest("hex")}`;
  }

  _signContextOperation({
    domain,
    method,
    params,
    graph,
    idempotencyKey,
    signerId,
    signerKey,
    requireContextPrincipal = true,
  }) {
    if (
      typeof signerId !== "string" ||
      !signerId.trim() ||
      typeof signerKey !== "string" ||
      !signerKey
    ) {
      throw new Error("operation signer id and key must be non-empty");
    }
    if (requireContextPrincipal && signerId !== this.verifiedContext.principal) {
      throw new Error("identity signer must match the verified principal");
    }
    const body = encode({ method, params });
    const parts = [];
    for (const value of [
      domain,
      this.verifiedContext.principal,
      this.verifiedContext.tenant,
      this.verifiedContext.audience,
      this.verifiedContext.agent_id,
    ]) {
      appendOperationBytes(parts, Buffer.from(value, "utf8"));
    }
    appendOperationList(parts, this.verifiedContext.roles);
    appendOperationList(parts, this.verifiedContext.scopes);
    appendOperationBytes(parts, Buffer.from(this.verifiedContext.policy_version, "utf8"));
    appendOperationList(parts, this.verifiedContext.delegation);
    appendOperationBytes(parts, Buffer.from(idempotencyKey, "utf8"));
    appendOperationBytes(parts, Buffer.from(graph, "utf8"));
    appendOperationBytes(parts, body);
    const digest = crypto.createHash("sha256").update(Buffer.concat(parts)).digest();
    const tag = crypto.createHmac("sha256", signerKey).update(digest).digest("hex");
    return `${signerId}:${tag}`;
  }

  registerIdentity({ agentId, role, teams, roles, signerId, signerKey }) {
    if (typeof agentId !== "string" || !agentId.trim()) {
      throw new Error("agentId must be a non-empty opaque identifier");
    }
    const idempotencyKey = this._newOperationIdempotencyKey();
    const params = {
      agent_id: agentId,
      role: normalizeAgentRole(role),
      teams: validateExplicitStringList("teams", teams),
      signature: "",
      roles: validateExplicitStringList("roles", roles),
    };
    params.signature = this._signContextOperation({
      domain: "eg-register-identity-v2",
      method: "RegisterIdentity",
      params: { ...params, signature: "" },
      graph: "__commons__",
      idempotencyKey,
      signerId,
      signerKey,
    });
    return this._send("RegisterIdentity", params, "__commons__", idempotencyKey);
  }

  bootstrapSystemIdentity({ agentId, signerId, signerKey }) {
    const context = this.verifiedContext;
    if (
      context.principal !== agentId ||
      context.agent_id !== agentId ||
      signerId !== agentId ||
      context.roles.length !== 0 ||
      context.scopes.length !== 1 ||
      context.scopes[0] !== "security:bootstrap" ||
      context.delegation.length !== 0
    ) {
      throw new Error(
        "bootstrap requires matching explicit identities and only security:bootstrap authority",
      );
    }
    return this.registerIdentity({
      agentId,
      role: "System",
      teams: [],
      roles: [],
      signerId,
      signerKey,
    });
  }

  applyMultisigMutation({ signerKeys, threshold, mutationType, query }) {
    if (
      signerKeys === null ||
      typeof signerKeys !== "object" ||
      Array.isArray(signerKeys) ||
      !Number.isInteger(threshold) ||
      threshold <= 0 ||
      Object.keys(signerKeys).length < threshold
    ) {
      throw new Error("threshold requires at least that many explicit signers");
    }
    if (
      typeof mutationType !== "string" ||
      !mutationType.trim() ||
      typeof query !== "string" ||
      !query.trim()
    ) {
      throw new Error("mutationType and query must be non-empty strings");
    }
    if (
      Object.entries(signerKeys).some(
        ([signerId, signerKey]) =>
          !signerId.trim() || typeof signerKey !== "string" || !signerKey,
      )
    ) {
      throw new Error("operation signer ids and keys must be non-empty strings");
    }
    const idempotencyKey = this._newOperationIdempotencyKey();
    const unsigned = {
      signatures: [],
      threshold,
      mutation_type: mutationType,
      query,
    };
    const signatures = Object.keys(signerKeys)
      .sort()
      .map((signerId) =>
        this._signContextOperation({
          domain: "eg-multisig-mutation-v2",
          method: "ApplyMultisigMutation",
          params: unsigned,
          graph: "__commons__",
          idempotencyKey,
          signerId,
          signerKey: signerKeys[signerId],
          requireContextPrincipal: false,
        }),
      );
    return this._send(
      "ApplyMultisigMutation",
      { ...unsigned, signatures },
      "__commons__",
      idempotencyKey,
    );
  }

  close() {
    if (this._sock) this._sock.end();
  }

  // ── Broker: exchanges / queues / publish (EG-275..280) ──────────────────────
  declareExchange(exchange, kind = "direct") {
    return this._send("DeclareExchange", { exchange, kind });
  }
  deleteExchange(exchange) {
    return this._send("DeleteExchange", { exchange });
  }
  bindQueue(exchange, queue, routing_key) {
    return this._send("BindQueue", { exchange, queue, routing_key });
  }
  unbindQueue(exchange, queue, routing_key) {
    return this._send("UnbindQueue", { exchange, queue, routing_key });
  }
  declareQueue(queue, policy = {}) {
    return this._send("DeclareQueue", {
      queue,
      dl_exchange: policy.dlExchange ?? null,
      dl_routing_key: policy.dlRoutingKey ?? null,
      max_delivery_count: policy.maxDeliveryCount ?? null,
      message_ttl_ms: policy.messageTtlMs ?? null,
      queue_expiry_ms: policy.queueExpiryMs ?? null,
      max_priority: policy.maxPriority ?? null,
    });
  }
  publish(exchange, routing_key, payload) {
    return this._send("Publish", { exchange, routing_key, payload });
  }
  publishEx(exchange, routing_key, payload, o = {}) {
    return this._send("PublishEx", {
      exchange,
      routing_key,
      payload,
      priority: o.priority ?? 0,
      delay_ms: o.delayMs ?? null,
      ttl_ms: o.ttlMs ?? null,
      now_ms: o.nowMs ?? null,
    });
  }
  publishConfirmed(exchange, routing_key, payload, o = {}) {
    return this._send("PublishConfirmed", {
      exchange,
      routing_key,
      payload,
      priority: o.priority ?? 0,
      delay_ms: o.delayMs ?? null,
      ttl_ms: o.ttlMs ?? null,
      now_ms: o.nowMs ?? null,
    });
  }
  publishIdempotent(exchange, routing_key, payload, o = {}) {
    return this._send("PublishIdempotent", {
      exchange,
      routing_key,
      payload,
      producer_id: o.producerId ?? null,
      seq: o.seq ?? 0,
      priority: o.priority ?? 0,
      delay_ms: o.delayMs ?? null,
      ttl_ms: o.ttlMs ?? null,
      now_ms: o.nowMs ?? null,
    });
  }
  // Consume / ack / reject (EG-280/276/284)
  brokerConsume(queue, { group, consumer, nowMs, leaseMs = 0, prefetch = 0 }) {
    return this._send("BrokerConsume", {
      queue,
      group,
      consumer,
      now_ms: nowMs,
      lease_ms: leaseMs,
      prefetch,
    });
  }
  brokerAck(queue, node_id) {
    return this._send("BrokerAck", { queue, node_id });
  }
  brokerReject(queue, node_id, { requeue, nowMs }) {
    return this._send("BrokerReject", { queue, node_id, requeue, now_ms: nowMs });
  }
  brokerAckTag(delivery_tag, { consumer }) {
    return this._send("BrokerAckTag", { delivery_tag, consumer });
  }
  brokerNackTag(delivery_tag, { consumer, requeue, nowMs }) {
    return this._send("BrokerNackTag", { delivery_tag, consumer, requeue, now_ms: nowMs });
  }
  brokerRenewTag(delivery_tag, { consumer, nowMs, leaseMs }) {
    return this._send("BrokerRenewTag", {
      delivery_tag,
      consumer,
      now_ms: nowMs,
      lease_ms: leaseMs,
    });
  }
  sweepExpired(nowMs) {
    return this._send("SweepExpired", { now_ms: nowMs });
  }
  // ── Replayable append-log streams (EG-283) ──────────────────────────────────
  streamDeclare(stream, { maxMessages = null, maxAgeMs = null } = {}) {
    return this._send("StreamDeclare", {
      stream,
      max_messages: maxMessages,
      max_age_ms: maxAgeMs,
    });
  }
  streamPublish(stream, payload, nowMs) {
    return this._send("StreamPublish", { stream, payload, now_ms: nowMs });
  }
  streamRead(stream, { fromOffset = 0, max = 0 } = {}) {
    return this._send("StreamRead", { stream, from_offset: fromOffset, max });
  }
  streamTrim(stream, nowMs) {
    return this._send("StreamTrim", { stream, now_ms: nowMs });
  }
  streamCommitOffset(stream, group, offset) {
    return this._send("StreamCommitOffset", { stream, group, offset });
  }
  streamCommittedOffset(stream, group) {
    return this._send("StreamCommittedOffset", { stream, group });
  }

  // ── RBAC admin (EG-092) — externally-tagged RbacAdminOp / ResourceSelector ──
  rbacAddRole(name, parents = []) {
    return this._send("RbacAdmin", { op: { AddRole: { name, parents } } });
  }
  rbacRemoveRole(name) {
    return this._send("RbacAdmin", { op: { RemoveRole: name } });
  }
  rbacAddGrant(role, resource, action, effect = "Allow") {
    return this._send("RbacAdmin", { op: { AddGrant: { role, resource, action, effect } } });
  }
  rbacRemoveGrant(role, resource, action, effect = "Allow") {
    return this._send("RbacAdmin", {
      op: { RemoveGrant: { role, resource, action, effect } },
    });
  }
  rbacList() {
    return this._send("RbacAdmin", { op: "List" });
  }

  // ── Ops: online backup / restore (EG-090) ───────────────────────────────────
  backup(destination, label = null) {
    return this._send("Backup", { destination, label });
  }
  restore(source, targetShards) {
    if (!Number.isInteger(targetShards) || targetShards < 1 || targetShards > 64) {
      throw new TypeError("targetShards must be an integer between 1 and 64");
    }
    return this._send("Restore", { source, target_shards: targetShards });
  }

  // ── NL->query (EG-080) ──────────────────────────────────────────────────────
  nlQuery(text, graph) {
    const targetGraph = graph || this.graph;
    return this._send("NlQuery", { text, graph: targetGraph }, targetGraph);
  }
}

export default EpistemicGraphThinClient;
