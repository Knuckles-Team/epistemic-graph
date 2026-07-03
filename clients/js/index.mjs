// CONCEPT:EG-3.1 — Thin JS/Node client for the Program-B engine Methods (B1.7).
//
// This is a DELIBERATELY THIN binding, NOT a full SDK. It covers ONLY the new wire
// `Method`s that had no client surface — the broker + append-log streams
// (EG-275..284/314), RBAC admin (EG-092), online backup/restore (EG-090), and
// NL->query (EG-080) — over the SAME framed-MessagePack transport the Python client
// uses (4-byte big-endian length prefix + a msgpack `{id, graph, auth_token, method,
// params}` request; responses demuxed by `id`; HMAC-SHA256(secret, str(id)) auth).
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

export class EpistemicGraphThinClient {
  /**
   * @param {object} opts
   * @param {string} [opts.socketPath] Unix domain socket path (default GRAPH_SERVICE_SOCKET).
   * @param {string} [opts.host] TCP host (alternative to socketPath).
   * @param {number} [opts.port] TCP port.
   * @param {string} [opts.authSecret] HMAC secret (default GRAPH_SERVICE_AUTH_SECRET).
   * @param {string} [opts.graph] Default graph name for requests.
   */
  constructor(opts = {}) {
    this.socketPath =
      opts.socketPath ||
      (opts.host ? null : process.env.GRAPH_SERVICE_SOCKET || "/tmp/epistemic-graph.sock");
    this.host = opts.host || null;
    this.port = opts.port || null;
    this.authSecret = opts.authSecret ?? process.env.GRAPH_SERVICE_AUTH_SECRET ?? "";
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
        for (const { reject: rj } of this._pending.values())
          rj(new Error("connection closed"));
        this._pending.clear();
      });
    });
  }

  _onData(chunk) {
    this._buf = Buffer.concat([this._buf, chunk]);
    // Frames: 4-byte big-endian length prefix + msgpack body.
    while (this._buf.length >= 4) {
      const len = this._buf.readUInt32BE(0);
      if (this._buf.length < 4 + len) break;
      const body = this._buf.subarray(4, 4 + len);
      this._buf = this._buf.subarray(4 + len);
      let resp;
      try {
        resp = decode(body);
      } catch (e) {
        continue;
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

  /** Send one request and await its correlated response. */
  send(method, params, graph) {
    const id = ++this._id;
    const req = {
      id,
      graph: graph || this.graph,
      auth_token: this.authSecret
        ? crypto.createHmac("sha256", this.authSecret).update(String(id)).digest("hex")
        : "",
      method,
    };
    if (params !== undefined) req.params = params;
    const payload = encode(req);
    const frame = Buffer.alloc(4 + payload.length);
    frame.writeUInt32BE(payload.length, 0);
    Buffer.from(payload).copy(frame, 4);
    return new Promise((resolve, reject) => {
      this._pending.set(id, { resolve, reject });
      this._sock.write(frame);
    });
  }

  close() {
    if (this._sock) this._sock.end();
  }

  // ── Broker: exchanges / queues / publish (EG-275..280) ──────────────────────
  declareExchange(exchange, kind = "direct") {
    return this.send("DeclareExchange", { exchange, kind });
  }
  deleteExchange(exchange) {
    return this.send("DeleteExchange", { exchange });
  }
  bindQueue(exchange, queue, routing_key) {
    return this.send("BindQueue", { exchange, queue, routing_key });
  }
  unbindQueue(exchange, queue, routing_key) {
    return this.send("UnbindQueue", { exchange, queue, routing_key });
  }
  declareQueue(queue, policy = {}) {
    return this.send("DeclareQueue", {
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
    return this.send("Publish", { exchange, routing_key, payload });
  }
  publishEx(exchange, routing_key, payload, o = {}) {
    return this.send("PublishEx", {
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
    return this.send("PublishConfirmed", {
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
    return this.send("PublishIdempotent", {
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
    return this.send("BrokerConsume", {
      queue,
      group,
      consumer,
      now_ms: nowMs,
      lease_ms: leaseMs,
      prefetch,
    });
  }
  brokerAck(queue, node_id) {
    return this.send("BrokerAck", { queue, node_id });
  }
  brokerReject(queue, node_id, { requeue, nowMs }) {
    return this.send("BrokerReject", { queue, node_id, requeue, now_ms: nowMs });
  }
  brokerAckTag(delivery_tag) {
    return this.send("BrokerAckTag", { delivery_tag });
  }
  brokerNackTag(delivery_tag, { requeue, nowMs }) {
    return this.send("BrokerNackTag", { delivery_tag, requeue, now_ms: nowMs });
  }
  sweepExpired(nowMs) {
    return this.send("SweepExpired", { now_ms: nowMs });
  }
  // ── Replayable append-log streams (EG-283) ──────────────────────────────────
  streamDeclare(stream, { maxMessages = null, maxAgeMs = null } = {}) {
    return this.send("StreamDeclare", {
      stream,
      max_messages: maxMessages,
      max_age_ms: maxAgeMs,
    });
  }
  streamPublish(stream, payload, nowMs) {
    return this.send("StreamPublish", { stream, payload, now_ms: nowMs });
  }
  streamRead(stream, { fromOffset = 0, max = 0 } = {}) {
    return this.send("StreamRead", { stream, from_offset: fromOffset, max });
  }
  streamTrim(stream, nowMs) {
    return this.send("StreamTrim", { stream, now_ms: nowMs });
  }
  streamCommitOffset(stream, group, offset) {
    return this.send("StreamCommitOffset", { stream, group, offset });
  }
  streamCommittedOffset(stream, group) {
    return this.send("StreamCommittedOffset", { stream, group });
  }

  // ── RBAC admin (EG-092) — externally-tagged RbacAdminOp / ResourceSelector ──
  rbacAddRole(name, parents = []) {
    return this.send("RbacAdmin", { op: { AddRole: { name, parents } } });
  }
  rbacRemoveRole(name) {
    return this.send("RbacAdmin", { op: { RemoveRole: name } });
  }
  rbacAddGrant(role, resource, action, effect = "Allow") {
    return this.send("RbacAdmin", { op: { AddGrant: { role, resource, action, effect } } });
  }
  rbacRemoveGrant(role, resource, action, effect = "Allow") {
    return this.send("RbacAdmin", {
      op: { RemoveGrant: { role, resource, action, effect } },
    });
  }
  rbacList() {
    return this.send("RbacAdmin", { op: "List" });
  }

  // ── Ops: online backup / restore (EG-090) ───────────────────────────────────
  backup(destination, label = null) {
    return this.send("Backup", { destination, label });
  }
  restore(source) {
    return this.send("Restore", { source });
  }

  // ── NL->query (EG-080) ──────────────────────────────────────────────────────
  nlQuery(text, graph) {
    return this.send("NlQuery", { text }, graph);
  }
}

export default EpistemicGraphThinClient;
