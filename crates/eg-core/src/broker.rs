//! Message-broker exchanges / routing primitives (CONCEPT:EG-275) — the
//! RabbitMQ-class layer on top of the native work-queue (CONCEPT:KG-2.303).
//!
//! ## What this is
//! A durable, pure-Rust broker modeled ENTIRELY as graph nodes on a control graph
//! (the same `__control__`/commons graph the KG-2.303 task queue lives on) — NO new
//! storage engine, NO parallel mechanism. Exchanges and bindings are ordinary nodes;
//! a queue's messages are pending nodes labeled `qmsg:<queue>` so that the existing
//! atomic [`GraphCore::claim_next_fields`](crate::graph::GraphCore::claim_next_fields)
//! (CONCEPT:KG-2.303) delivers them FIFO and a compare-and-set acks them. Publishing
//! resolves the exchange's bindings through the pure [`route`] matcher and appends one
//! pending message to every matched queue atomically under one write guard
//! ([`GraphCore::broker_enqueue`](crate::graph::GraphCore::broker_enqueue)).
//!
//! ## Layers
//!   * PURE (no graph): [`ExchangeKind`], [`Exchange`], [`Binding`], the AMQP topic
//!     wildcard matcher [`topic_matches`] (`*` = one word, `#` = zero-or-more words),
//!     and [`route`] (kind + bindings + routing key → matched queues). Unit-tested in
//!     isolation.
//!   * GRAPH-BACKED (over a `&GraphCore`, reusing its PUBLIC node API + the KG-2.303
//!     claim/ack): [`declare_exchange`], [`delete_exchange`], [`bind_queue`],
//!     [`unbind_queue`], [`publish`]. `publish` is a deterministic pure function of
//!     graph state (routing reads current bindings; seq comes from a durable counter),
//!     so a WAL/Raft replay of `Method::Publish` reproduces identical nodes.
//!
//! No new heavy dependency is pulled in — only `serde`/`serde_json`/`rmp_serde`, which
//! `eg-core` already links (the Pi contract holds; the AMQP socket adapter lives in the
//! server crate behind the `amqp-wire` feature).

use serde::{Deserialize, Serialize};

use crate::graph::GraphCore;

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
/// `claim_next_fields` scans to deliver the queue FIFO (CONCEPT:KG-2.303).
pub fn queue_msg_label(queue: &str) -> String {
    format!("qmsg:{queue}")
}

/// Node id for the `seq`-th message appended to `queue`.
pub fn message_node_id(queue: &str, seq: i64) -> String {
    format!("broker:msg:{queue}:{seq}")
}

const EXCHANGE_TYPE: &str = "BrokerExchange";
const BINDING_TYPE: &str = "BrokerBinding";
const QUEUE_SEQ_TYPE: &str = "BrokerQueueSeq";

// ── Pure primitives ──────────────────────────────────────────────────────

/// The three routing disciplines (CONCEPT:EG-275), mirroring AMQP 0.9.1.
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

/// A durable exchange definition (CONCEPT:EG-275).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exchange {
    pub name: String,
    pub kind: ExchangeKind,
}

/// A durable exchange→queue binding (CONCEPT:EG-275). For a topic exchange the
/// `routing_key` is a `*`/`#` pattern; for direct it is an exact key; for fanout it
/// is ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub exchange: String,
    pub queue: String,
    pub routing_key: String,
}

/// AMQP 0.9.1 topic wildcard match (CONCEPT:EG-275). Both `pattern` and `key` are
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

/// Recursive word-list matcher backing [`topic_matches`]. Kept private + total.
fn topic_matches_words(pattern: &[&str], key: &[&str]) -> bool {
    match pattern.split_first() {
        // Pattern exhausted: match iff the key is also exhausted.
        None => key.is_empty(),
        Some((&"#", rest)) => {
            // `#` matches zero words (advance the pattern past it) …
            if topic_matches_words(rest, key) {
                return true;
            }
            // … or one-or-more words (consume a key word, keep the `#`).
            !key.is_empty() && topic_matches_words(pattern, &key[1..])
        }
        Some((&"*", rest)) => {
            // `*` matches exactly one word.
            !key.is_empty() && topic_matches_words(rest, &key[1..])
        }
        Some((&word, rest)) => {
            // A literal word must match the head key word exactly.
            !key.is_empty() && word == key[0] && topic_matches_words(rest, &key[1..])
        }
    }
}

/// Resolve a published `routing_key` against an exchange's `kind` + `bindings` to the
/// set of destination queues (CONCEPT:EG-275) — the PURE routing core. Order-stable
/// (bindings order) and de-duplicated (a queue bound twice is enqueued once).
pub fn route(kind: ExchangeKind, bindings: &[Binding], routing_key: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for b in bindings {
        let hit = match kind {
            ExchangeKind::Fanout => true,
            ExchangeKind::Direct => b.routing_key == routing_key,
            ExchangeKind::Topic => topic_matches(&b.routing_key, routing_key),
        };
        if hit && !out.contains(&b.queue) {
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
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
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

fn node_object(core: &GraphCore, id: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let blob = core.get_node_properties(id)?;
    match rmp_serde::from_slice::<serde_json::Value>(&blob) {
        Ok(serde_json::Value::Object(o)) => Some(o),
        _ => None,
    }
}

fn to_msgpack(v: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(v).unwrap_or_default()
}

/// Declare (idempotently upsert) an exchange (CONCEPT:EG-275). Re-declaring with the
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

/// Delete an exchange and ALL of its bindings (CONCEPT:EG-275). Returns whether the
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

/// Bind `queue` to `exchange` under `routing_key` (CONCEPT:EG-275), idempotently. Also
/// ensures the queue's durable sequence counter node exists so publishes start at 0.
pub fn bind_queue(core: &GraphCore, exchange: &str, queue: &str, routing_key: &str) {
    ensure_queue_seq(core, queue);
    let props = serde_json::json!({
        "type": BINDING_TYPE,
        "exchange": exchange,
        "queue": queue,
        "routing_key": routing_key,
    });
    core.add_node(binding_node_id(exchange, queue, routing_key), to_msgpack(&props));
}

/// Remove a specific `exchange`/`queue`/`routing_key` binding (CONCEPT:EG-275).
/// Returns whether a matching binding existed.
pub fn unbind_queue(core: &GraphCore, exchange: &str, queue: &str, routing_key: &str) -> bool {
    let id = binding_node_id(exchange, queue, routing_key);
    let existed = core.has_node(&id);
    if existed {
        core.remove_node(id);
    }
    existed
}

/// All bindings currently attached to `exchange` (CONCEPT:EG-275).
pub fn load_bindings(core: &GraphCore, exchange: &str) -> Vec<Binding> {
    core.get_nodes_by_label(BINDING_TYPE, 0)
        .into_iter()
        .filter_map(|(_, blob)| {
            let v = rmp_serde::from_slice::<serde_json::Value>(&blob).ok()?;
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
    if !core.has_node(&id) {
        let props = serde_json::json!({
            "type": QUEUE_SEQ_TYPE,
            "queue": queue,
            "next_seq": 0,
        });
        core.add_node(id, to_msgpack(&props));
    }
}

/// Publish `payload` to `exchange` with `routing_key` (CONCEPT:EG-275). Resolves the
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── CONCEPT:EG-275 topic wildcard matcher ────────────────────────────

    #[test]
    fn eg275_topic_exact_match() {
        assert!(topic_matches("stock.usd.nyse", "stock.usd.nyse"));
        assert!(!topic_matches("stock.usd.nyse", "stock.usd.nasdaq"));
    }

    #[test]
    fn eg275_topic_star_matches_exactly_one_word() {
        assert!(topic_matches("stock.*.nyse", "stock.usd.nyse"));
        assert!(!topic_matches("stock.*.nyse", "stock.usd.eur.nyse"));
        assert!(!topic_matches("stock.*", "stock")); // * needs one word
        assert!(topic_matches("*.*.*", "a.b.c"));
        assert!(!topic_matches("*.*.*", "a.b"));
    }

    #[test]
    fn eg275_topic_hash_matches_zero_or_more_words() {
        assert!(topic_matches("stock.#", "stock")); // zero words
        assert!(topic_matches("stock.#", "stock.usd")); // one
        assert!(topic_matches("stock.#", "stock.usd.nyse")); // many
        assert!(topic_matches("#", "")); // # matches empty
        assert!(topic_matches("#", "a.b.c.d"));
    }

    #[test]
    fn eg275_topic_hash_in_the_middle_and_leading() {
        assert!(topic_matches("stock.#.nyse", "stock.nyse")); // # = zero
        assert!(topic_matches("stock.#.nyse", "stock.usd.nyse")); // # = one
        assert!(topic_matches("stock.#.nyse", "stock.usd.eur.nyse")); // # = two
        assert!(!topic_matches("stock.#.nyse", "stock.usd.eur")); // wrong tail
        assert!(topic_matches("#.nyse", "a.b.nyse"));
        assert!(topic_matches("#.nyse", "nyse"));
    }

    #[test]
    fn eg275_topic_adjacent_hashes_and_mixed() {
        assert!(topic_matches("#.#", "a.b.c"));
        assert!(topic_matches("#.#", ""));
        assert!(topic_matches("*.#", "a.b.c"));
        assert!(topic_matches("*.#", "a"));
        assert!(!topic_matches("*.#", "")); // * still needs one word
    }

    // ── CONCEPT:EG-275 pure route() over kinds ────────────────────────────

    fn b(q: &str, rk: &str) -> Binding {
        Binding {
            exchange: "ex".into(),
            queue: q.into(),
            routing_key: rk.into(),
        }
    }

    #[test]
    fn eg275_route_direct_exact_key() {
        let bindings = vec![b("q1", "info"), b("q2", "error"), b("q3", "info")];
        assert_eq!(
            route(ExchangeKind::Direct, &bindings, "info"),
            vec!["q1".to_string(), "q3".to_string()]
        );
        assert_eq!(route(ExchangeKind::Direct, &bindings, "warn"), Vec::<String>::new());
    }

    #[test]
    fn eg275_route_fanout_ignores_key() {
        let bindings = vec![b("q1", "whatever"), b("q2", "")];
        assert_eq!(
            route(ExchangeKind::Fanout, &bindings, "anything"),
            vec!["q1".to_string(), "q2".to_string()]
        );
    }

    #[test]
    fn eg275_route_topic_uses_wildcards() {
        let bindings = vec![b("all", "stock.#"), b("usd", "stock.usd.*"), b("err", "log.error")];
        assert_eq!(
            route(ExchangeKind::Topic, &bindings, "stock.usd.nyse"),
            vec!["all".to_string(), "usd".to_string()]
        );
        assert_eq!(
            route(ExchangeKind::Topic, &bindings, "log.error"),
            vec!["err".to_string()]
        );
    }

    #[test]
    fn eg275_route_dedups_queue_bound_twice() {
        let bindings = vec![b("q1", "a"), b("q1", "b")];
        assert_eq!(route(ExchangeKind::Fanout, &bindings, "x"), vec!["q1".to_string()]);
    }

    // ── CONCEPT:EG-275 hex payload round-trip ─────────────────────────────

    #[test]
    fn eg275_hex_roundtrips_binary_payload() {
        for payload in [
            b"".to_vec(),
            b"hello".to_vec(),
            vec![0u8, 1, 2, 254, 255, 128, 16],
        ] {
            let enc = hex_encode(&payload);
            assert_eq!(hex_decode(&enc), Some(payload));
        }
        assert_eq!(hex_decode("xyz"), None); // malformed
        assert_eq!(hex_decode("abc"), None); // odd length
    }

    // ── CONCEPT:EG-275 graph-backed declare/bind/publish/consume ──────────

    #[test]
    fn eg275_declare_is_idempotent_and_kind_locked() {
        let core = GraphCore::new();
        assert!(declare_exchange(&core, "ex", ExchangeKind::Topic).is_ok());
        assert!(declare_exchange(&core, "ex", ExchangeKind::Topic).is_ok()); // no-op
        assert_eq!(load_exchange_kind(&core, "ex"), Some(ExchangeKind::Topic));
        assert!(declare_exchange(&core, "ex", ExchangeKind::Direct).is_err()); // conflict
    }

    #[test]
    fn eg275_publish_routes_to_bound_queues_and_claim_delivers_fifo() {
        let core = GraphCore::new();
        declare_exchange(&core, "logs", ExchangeKind::Topic).unwrap();
        bind_queue(&core, "logs", "all", "log.#");
        bind_queue(&core, "logs", "errs", "log.error");

        // one message routed to BOTH queues
        assert_eq!(publish(&core, "logs", "log.error", b"boom"), 2);
        // a second message only to `all`
        assert_eq!(publish(&core, "logs", "log.info", b"fyi"), 1);
        // unroutable
        assert_eq!(publish(&core, "logs", "metric.cpu", b"x"), 0);

        // consume `errs` via the KG-2.303 claim: FIFO, exactly one pending message
        let claim = serde_json::json!({"status": "claimed"})
            .as_object()
            .unwrap()
            .clone();
        let got = core.claim_next_fields(&queue_msg_label("errs"), &claim);
        assert!(got.is_some(), "errs must have one deliverable message");
        let (_, props) = got.unwrap();
        let hexed = props.get("payload").and_then(|v| v.as_str()).unwrap();
        assert_eq!(hex_decode(hexed), Some(b"boom".to_vec()));
        // no more pending on errs
        assert!(core.claim_next_fields(&queue_msg_label("errs"), &claim).is_none());

        // `all` has two messages, delivered in publish order (seq monotonic)
        let (_, p1) = core.claim_next_fields(&queue_msg_label("all"), &claim).unwrap();
        let (_, p2) = core.claim_next_fields(&queue_msg_label("all"), &claim).unwrap();
        assert_eq!(p1.get("seq").and_then(|s| s.as_i64()), Some(0));
        assert_eq!(p2.get("seq").and_then(|s| s.as_i64()), Some(1));
    }

    #[test]
    fn eg275_unbind_and_delete_stop_routing() {
        let core = GraphCore::new();
        declare_exchange(&core, "ex", ExchangeKind::Direct).unwrap();
        bind_queue(&core, "ex", "q", "k");
        assert_eq!(publish(&core, "ex", "k", b"1"), 1);
        assert!(unbind_queue(&core, "ex", "q", "k"));
        assert_eq!(publish(&core, "ex", "k", b"2"), 0); // no binding
        assert!(delete_exchange(&core, "ex"));
        assert_eq!(publish(&core, "ex", "k", b"3"), 0); // no exchange
        assert!(load_exchange_kind(&core, "ex").is_none());
    }
}
