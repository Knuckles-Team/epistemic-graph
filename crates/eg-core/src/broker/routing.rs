use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::graph::GraphCore;

pub(crate) fn decode_property(bytes: &[u8]) -> Result<serde_json::Value, ()> {
    eg_types::msgpack::decode_property_value(bytes).map_err(|_| ())
}

// ── Node-id + label conventions (single source of truth) ─────────────────

/// Node id for an exchange definition node.
pub fn exchange_node_id(name: &str) -> String {
    format!("broker:ex:{name}")
}

/// Node id for a binding definition node. The `\u{1}` field delimiter is a control
/// char that cannot appear in a routing key / name, so the composite id is unique and
/// reversible for [`unbind_queue`].
pub fn binding_node_id(exchange: &str, queue: &str, routing_key: &str) -> String {
    format!("broker:bind:{exchange}\u{1}{queue}\u{1}{routing_key}")
}

/// Node id for a queue's durable monotonic sequence counter.
pub fn queue_seq_node_id(queue: &str) -> String {
    format!("broker:seq:{queue}")
}

/// The `type`/label a queue's pending message nodes carry — the label
/// `claim_next_fields` scans to deliver the queue FIFO (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending).
pub fn queue_msg_label(queue: &str) -> String {
    format!("qmsg:{queue}")
}

/// Node id for the `seq`-th message appended to `queue`.
pub fn message_node_id(queue: &str, seq: i64) -> String {
    format!("broker:msg:{queue}:{seq}")
}

const EXCHANGE_TYPE: &str = "BrokerExchange";
const BINDING_TYPE: &str = "BrokerBinding";
pub(super) const QUEUE_SEQ_TYPE: &str = "BrokerQueueSeq";

// ── Pure primitives ──────────────────────────────────────────────────────

/// The three routing disciplines (CONCEPT:EG-KG.compute.message-broker-exchanges), mirroring AMQP 0.9.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeKind {
    /// Deliver to queues bound with a routing key EQUAL to the message's.
    Direct,
    /// Deliver to queues whose binding pattern matches via `*`/`#` wildcards.
    Topic,
    /// Deliver to EVERY bound queue, ignoring the routing key.
    Fanout,
}

impl ExchangeKind {
    /// Parse the wire spelling (`direct`/`topic`/`fanout`, case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "direct" => Some(Self::Direct),
            "topic" => Some(Self::Topic),
            "fanout" => Some(Self::Fanout),
            _ => None,
        }
    }

    /// The canonical lowercase wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Topic => "topic",
            Self::Fanout => "fanout",
        }
    }
}

/// A durable exchange definition (CONCEPT:EG-KG.compute.message-broker-exchanges).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exchange {
    pub name: String,
    pub kind: ExchangeKind,
}

/// A durable exchange→queue binding (CONCEPT:EG-KG.compute.message-broker-exchanges). For a topic exchange the
/// `routing_key` is a `*`/`#` pattern; for direct it is an exact key; for fanout it
/// is ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub exchange: String,
    pub queue: String,
    pub routing_key: String,
}

/// AMQP 0.9.1 topic wildcard match (CONCEPT:EG-KG.compute.message-broker-exchanges). Both `pattern` and `key` are
/// dot-delimited word lists; `*` matches EXACTLY one word and `#` matches ZERO OR MORE
/// words. Correct for the tricky cases (`#` at either end, adjacent `#`, empty key).
pub fn topic_matches(pattern: &str, key: &str) -> bool {
    let p: Vec<&str> = if pattern.is_empty() {
        Vec::new()
    } else {
        pattern.split('.').collect()
    };
    let k: Vec<&str> = if key.is_empty() {
        Vec::new()
    } else {
        key.split('.').collect()
    };
    topic_matches_words(&p, &k)
}

/// Iterative NFA-style matcher backing [`topic_matches`].
///
/// Every reachable pattern position is represented at most once for each key
/// word. That makes ambiguous chains of `#` polynomial (`O(P * K)` worst case,
/// `O(P)` memory) instead of recursively enumerating exponentially many ways to
/// partition the key. In the common case the frontier is small, so work is
/// proportional to the states that are actually reachable. It also avoids a
/// caller-controlled recursion depth on the MQTT/AMQP ingress path.
fn topic_matches_words(pattern: &[&str], key: &[&str]) -> bool {
    fn push_epsilon_closure(
        pattern: &[&str],
        start: usize,
        generation: usize,
        seen: &mut [usize],
        out: &mut Vec<usize>,
    ) {
        let mut state = start;
        loop {
            // A prior seed already walked this state's complete consecutive-`#`
            // closure during the current generation.
            if seen[state] == generation {
                return;
            }
            seen[state] = generation;
            out.push(state);
            if state == pattern.len() || pattern[state] != "#" {
                return;
            }
            // `#` may consume zero words, so the following state is reachable
            // before the next input word is consumed.
            state += 1;
        }
    }

    let mut seen = vec![0usize; pattern.len() + 1];
    let mut generation = 1usize;
    let mut active = Vec::with_capacity(pattern.len() + 1);
    let mut next = Vec::with_capacity(pattern.len() + 1);
    push_epsilon_closure(pattern, 0, generation, &mut seen, &mut active);

    for word in key {
        generation += 1;
        next.clear();
        for &state in &active {
            if state == pattern.len() {
                continue;
            }
            match pattern[state] {
                // Consume one word while remaining at `#`; its epsilon closure
                // also makes every following consecutive `#` reachable.
                "#" => push_epsilon_closure(pattern, state, generation, &mut seen, &mut next),
                "*" => push_epsilon_closure(pattern, state + 1, generation, &mut seen, &mut next),
                literal if literal == *word => {
                    push_epsilon_closure(pattern, state + 1, generation, &mut seen, &mut next)
                }
                _ => {}
            }
        }
        if next.is_empty() {
            return false;
        }
        std::mem::swap(&mut active, &mut next);
    }

    active.contains(&pattern.len())
}

/// Resolve a published `routing_key` against an exchange's `kind` + `bindings` to the
/// set of destination queues (CONCEPT:EG-KG.compute.message-broker-exchanges) — the PURE routing core. Order-stable
/// (bindings order) and de-duplicated (a queue bound twice is enqueued once).
pub fn route(kind: ExchangeKind, bindings: &[Binding], routing_key: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen_queues: HashSet<&str> = HashSet::with_capacity(bindings.len());
    for b in bindings {
        let hit = match kind {
            ExchangeKind::Fanout => true,
            ExchangeKind::Direct => b.routing_key == routing_key,
            ExchangeKind::Topic => topic_matches(&b.routing_key, routing_key),
        };
        if hit && seen_queues.insert(b.queue.as_str()) {
            out.push(b.queue.clone());
        }
    }
    out
}

// ── Hex payload codec (dep-free, exact round-trip) ────────────────────────

/// Lower-hex encode arbitrary bytes so a binary AMQP body round-trips through a JSON
/// node property with fidelity (no base64 dependency, Pi-contract clean).
pub fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(LUT[(b >> 4) as usize] as char);
        s.push(LUT[(b & 0x0f) as usize] as char);
    }
    s
}

/// Decode a [`hex_encode`] string back to bytes; `None` on any malformed input.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    fn nibble(c: u8) -> Option<u8> {
        char::from(c)
            .to_digit(16)
            .and_then(|value| u8::try_from(value).ok())
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i < b.len() {
        let hi = nibble(b[i])?;
        let lo = nibble(b[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

// ── Graph-backed operations (reuse GraphCore's public API + KG-2.303) ─────

pub(super) fn node_object(
    core: &GraphCore,
    id: &str,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let blob = core.get_node_properties(id)?;
    match decode_property(&blob) {
        Ok(serde_json::Value::Object(o)) => Some(o),
        _ => None,
    }
}

pub(crate) fn to_msgpack(v: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(v).unwrap_or_default()
}

/// Declare (idempotently upsert) an exchange (CONCEPT:EG-KG.compute.message-broker-exchanges). Re-declaring with the
/// SAME kind is a no-op success; re-declaring with a DIFFERENT kind is rejected
/// (AMQP `PRECONDITION_FAILED` semantics).
pub fn declare_exchange(core: &GraphCore, name: &str, kind: ExchangeKind) -> Result<(), String> {
    let id = exchange_node_id(name);
    if let Some(existing) = load_exchange_kind(core, name) {
        if existing != kind {
            return Err(format!(
                "exchange '{name}' already declared as '{}', cannot redeclare as '{}'",
                existing.as_str(),
                kind.as_str()
            ));
        }
        return Ok(());
    }
    let props = serde_json::json!({
        "type": EXCHANGE_TYPE,
        "name": name,
        "kind": kind.as_str(),
    });
    core.add_node(id, to_msgpack(&props));
    Ok(())
}

/// Read an exchange's kind, or `None` if it is not declared.
pub fn load_exchange_kind(core: &GraphCore, name: &str) -> Option<ExchangeKind> {
    let obj = node_object(core, &exchange_node_id(name))?;
    ExchangeKind::parse(obj.get("kind")?.as_str()?)
}

/// Delete an exchange and ALL of its bindings (CONCEPT:EG-KG.compute.message-broker-exchanges). Returns whether the
/// exchange existed. Queues + their messages are untouched (only the routing edges go).
pub fn delete_exchange(core: &GraphCore, name: &str) -> bool {
    let existed = core.has_node(&exchange_node_id(name));
    for b in load_bindings(core, name) {
        core.remove_node(binding_node_id(&b.exchange, &b.queue, &b.routing_key));
    }
    if existed {
        core.remove_node(exchange_node_id(name));
    }
    existed
}

/// Bind `queue` to `exchange` under `routing_key` (CONCEPT:EG-KG.compute.message-broker-exchanges), idempotently. Also
/// ensures the queue's durable sequence counter node exists so publishes start at 0.
pub fn bind_queue(core: &GraphCore, exchange: &str, queue: &str, routing_key: &str) {
    ensure_queue_seq(core, queue);
    let props = serde_json::json!({
        "type": BINDING_TYPE,
        "exchange": exchange,
        "queue": queue,
        "routing_key": routing_key,
    });
    core.add_node(
        binding_node_id(exchange, queue, routing_key),
        to_msgpack(&props),
    );
}

/// Remove a specific `exchange`/`queue`/`routing_key` binding (CONCEPT:EG-KG.compute.message-broker-exchanges).
/// Returns whether a matching binding existed.
pub fn unbind_queue(core: &GraphCore, exchange: &str, queue: &str, routing_key: &str) -> bool {
    let id = binding_node_id(exchange, queue, routing_key);
    let existed = core.has_node(&id);
    if existed {
        core.remove_node(id);
    }
    existed
}

/// All bindings currently attached to `exchange` (CONCEPT:EG-KG.compute.message-broker-exchanges).
pub fn load_bindings(core: &GraphCore, exchange: &str) -> Vec<Binding> {
    core.get_nodes_by_label(BINDING_TYPE, 0)
        .into_iter()
        .filter_map(|(_, blob)| {
            let v = decode_property(&blob).ok()?;
            let o = v.as_object()?;
            if o.get("exchange").and_then(|x| x.as_str()) != Some(exchange) {
                return None;
            }
            Some(Binding {
                exchange: exchange.to_string(),
                queue: o.get("queue")?.as_str()?.to_string(),
                routing_key: o.get("routing_key")?.as_str()?.to_string(),
            })
        })
        .collect()
}

/// Ensure a queue's durable monotonic-seq counter node exists (starting at 0). Called
/// on bind + on the publish path so an unbound-but-published queue is still monotonic.
pub fn ensure_queue_seq(core: &GraphCore, queue: &str) {
    let id = queue_seq_node_id(queue);
    super::ensure_counter_node(core, id, || {
        serde_json::json!({
            "type": QUEUE_SEQ_TYPE,
            "queue": queue,
            "next_seq": 0,
        })
    });
}

/// Publish `payload` to `exchange` with `routing_key` (CONCEPT:EG-KG.compute.message-broker-exchanges). Resolves the
/// destination queues through [`route`] over the exchange's current bindings, then
/// appends one pending message to EACH matched queue atomically under one write guard.
/// Returns the number of queues the message was delivered to (0 = unroutable / unknown
/// exchange). Deterministic: routing + seq derive only from graph state, so replaying
/// the same `Method::Publish` over the same pre-image reproduces identical message nodes.
pub fn publish(core: &GraphCore, exchange: &str, routing_key: &str, payload: &[u8]) -> usize {
    let Some(kind) = load_exchange_kind(core, exchange) else {
        return 0;
    };
    let bindings = load_bindings(core, exchange);
    let queues = route(kind, &bindings, routing_key);
    if queues.is_empty() {
        return 0;
    }
    let payload_hex = hex_encode(payload);
    core.broker_enqueue(&queues, exchange, routing_key, &payload_hex)
}
