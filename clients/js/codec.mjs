// The eg2. canonical method body, computed by the engine's own codec.
//
// The engine MACs the canonical body it re-derives from the request it DECODED:
// Rust declaration order, serde defaults materialized, maps sorted, float and byte
// widths per field. Restating that in JavaScript mis-signed most of the contract's
// method-body vectors, so the client runs the engine's own decoder and encoder:
// crates/eg-method-codec compiled to WebAssembly by
// scripts/build_method_codec_wasm.py (CI rebuilds it and byte-compares this copy).
// The module has no imports; Node's built-in WebAssembly runs it.

import { readFileSync } from "node:fs";
import { encode } from "@msgpack/msgpack";

const METHOD_BODY_CODEC = "eg/method-body/v1";
const BODY_STATUS = 0;
const MODULE_URL = new URL("./eg_method_codec.wasm", import.meta.url);
const utf8 = new TextDecoder("utf-8", { fatal: true });

let compiled = null;
// The module aborts on a panic without unwinding, so a trapped instance may hold
// its buffers locked: it is discarded and the next call instantiates a fresh one.
let instance = null;

function bytesAt(exports, ptr, length) {
  return new Uint8Array(exports.memory.buffer, ptr, length).slice();
}

function codecExports() {
  if (instance !== null) return instance;
  compiled ??= new WebAssembly.Module(readFileSync(MODULE_URL));
  const created = new WebAssembly.Instance(compiled, {}).exports;
  const identity = utf8.decode(bytesAt(created, created.eg_codec_ptr(), created.eg_codec_len()));
  if (identity !== METHOD_BODY_CODEC) {
    throw new Error(`the embedded method-body codec is not ${METHOD_BODY_CODEC}`);
  }
  instance = created;
  return instance;
}

function runCodec(frame) {
  const exports = codecExports();
  const ptr = exports.eg_input_reserve(frame.length);
  new Uint8Array(exports.memory.buffer, ptr, frame.length).set(frame);
  const status = exports.eg_canonical_body();
  return { status, output: bytesAt(exports, exports.eg_output_ptr(), exports.eg_output_len()) };
}

/**
 * The request frame the transport carries for one call, without its token. A unit
 * method (params `undefined`) sends no params key.
 */
export function requestFrame({ id = 0, graph = "", authToken = "", agentId = null, method, params }) {
  const frame = { id, graph, auth_token: authToken, agent_id: agentId, method };
  if (params !== undefined) frame.params = params;
  return frame;
}

/**
 * The body the engine binds into the eg2. MAC for one method call: the request
 * frame this client sends, decoded and re-encoded by the engine's own codec. The
 * frame's id, graph and token do not affect it.
 */
export function canonicalMethodBody(method, params) {
  const frame = encode(requestFrame({ method, params }));
  let result;
  try {
    result = runCodec(frame);
  } catch (error) {
    instance = null;
    throw error;
  }
  if (result.status !== BODY_STATUS) {
    throw new Error(`request is not a valid engine request: ${utf8.decode(result.output)}`);
  }
  return result.output;
}
