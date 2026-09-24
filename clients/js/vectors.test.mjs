// The engine MACs the canonical body it re-derives from the request it decoded.
// contract/fixtures/method_body_vectors.json is rendered by gen_contract from the
// engine's own decoder, encoder and envelope MAC: one client-shaped request per
// catalog method (alphabetical keys, defaults omitted) plus every typed contract
// sample, each with its golden body digest and the MAC the engine computes under
// one published envelope. Replaying each request through this client's signer must
// reproduce the engine's bytes and MAC exactly.

import assert from "node:assert/strict";
import crypto from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";
import { decode } from "@msgpack/msgpack";

import { canonicalMethodBody } from "./codec.mjs";
import { EpistemicGraphThinClient } from "./index.mjs";

function contractJson(name) {
  return JSON.parse(readFileSync(new URL(`../../contract/${name}`, import.meta.url), "utf8"));
}

const { envelope, vectors } = contractJson("fixtures/method_body_vectors.json");

function tokenMac(token) {
  return JSON.parse(Buffer.from(token.slice("eg2.".length), "hex").toString("utf8")).mac;
}

test("vectors cover every published method", () => {
  const covered = new Set(vectors.map((vector) => vector.method));
  const missing = contractJson("methods.json")
    .methods.map((method) => method.id)
    .filter((id) => !covered.has(id));
  assert.deepEqual(missing, []);
});

test("signer matches the engine for every method-body vector", async (t) => {
  assert.ok(vectors.length > 0 && envelope.secret, "the vector file carries vectors and an envelope");
  const client = new EpistemicGraphThinClient({
    host: "engine.invalid",
    port: 9100,
    authSecret: envelope.secret,
    verifiedContext: envelope.context,
  });
  const seal = { timestamp: envelope.timestamp, nonce: envelope.nonce };
  for (const vector of vectors) {
    await t.test(vector.label, () => {
      // What a caller hands the signer: the decoded params; a unit method has none.
      const { method, params } = decode(Buffer.from(vector.request_msgpack, "hex"));
      const body = canonicalMethodBody(method, params);
      assert.equal(body.length, vector.canonical_len);
      assert.equal(crypto.createHash("sha256").update(body).digest("hex"), vector.canonical_sha256);
      const token = client._seal(
        envelope.request_id,
        envelope.graph,
        method,
        body,
        envelope.idempotency_key,
        seal,
      );
      assert.equal(tokenMac(token), vector.mac);
    });
  }
});

test("signer refuses a request the engine cannot decode", () => {
  assert.throws(
    () => canonicalMethodBody("CancelRequest", { target_req_id: "seven" }),
    /^Error: request is not a valid engine request: /,
  );
  assert.ok(canonicalMethodBody("CancelRequest", { target_req_id: 7 }).length > 0);
});
