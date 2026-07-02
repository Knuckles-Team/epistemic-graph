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

// ══════════════════════════════════════════════════════════════════════════
// Broker policy extensions (CONCEPT:EG-276 DLQ / EG-277 TTL / EG-278 priority /
// EG-279 delay/schedule / EG-280 consumer-groups + QoS). Every addition here is
// ADDITIVE over EG-275: a queue with NO policy node and a message with NO
// priority/delay/expiry field claims/acks exactly as EG-275 does. Time is never
// read from a server clock — the caller passes `now_ms` explicitly (mirroring
// `InvalidateEdge`'s `tx_now`), so a WAL/Raft replay of `PublishEx`/`BrokerConsume`/
// `BrokerReject`/`SweepExpired` reproduces byte-identical graph state.
// ══════════════════════════════════════════════════════════════════════════

const QUEUE_POLICY_TYPE: &str = "BrokerQueuePolicy";

/// Node id for a queue's durable policy node (CONCEPT:EG-276/277/278).
pub fn queue_policy_node_id(queue: &str) -> String {
    format!("broker:qpolicy:{queue}")
}

/// A queue's durable policy (CONCEPT:EG-276 DLQ / EG-277 TTL / EG-278 priority).
/// Every field optional; an all-`None` policy (the default when no policy node
/// exists) makes the queue behave exactly as EG-275.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuePolicy {
    /// EG-276: exchange dead-lettered messages are republished to (`None` ⇒ drop).
    pub dl_exchange: Option<String>,
    /// EG-276: routing key for dead-lettered messages (`None` ⇒ reuse the original).
    pub dl_routing_key: Option<String>,
    /// EG-276: max delivery attempts before a message is dead-lettered.
    pub max_delivery_count: Option<u32>,
    /// EG-277: default per-message TTL (ms) applied when a publish omits its own.
    pub message_ttl_ms: Option<u64>,
    /// EG-277: queue-expiry hint (ms) — advisory, surfaced for tooling.
    pub queue_expiry_ms: Option<u64>,
    /// EG-278: max priority band the queue honors — advisory ceiling.
    pub max_priority: Option<u8>,
}

/// Declare (idempotently upsert) a queue's policy node (CONCEPT:EG-276/277/278).
/// Also ensures the queue's seq counter exists so a policy-only queue is publishable.
pub fn declare_queue(core: &GraphCore, queue: &str, policy: &QueuePolicy) {
    ensure_queue_seq(core, queue);
    let mut props = serde_json::Map::new();
    props.insert("type".into(), serde_json::Value::String(QUEUE_POLICY_TYPE.into()));
    props.insert("queue".into(), serde_json::Value::String(queue.into()));
    if let Ok(v) = serde_json::to_value(policy) {
        if let Some(o) = v.as_object() {
            for (k, val) in o {
                props.insert(k.clone(), val.clone());
            }
        }
    }
    core.add_node(
        queue_policy_node_id(queue),
        to_msgpack(&serde_json::Value::Object(props)),
    );
}

/// Read a queue's policy (CONCEPT:EG-276/277/278); an absent node ⇒ the default
/// all-`None` policy (EG-275 behavior).
pub fn load_queue_policy(core: &GraphCore, queue: &str) -> QueuePolicy {
    match node_object(core, &queue_policy_node_id(queue)) {
        Some(o) => serde_json::from_value(serde_json::Value::Object(o)).unwrap_or_default(),
        None => QueuePolicy::default(),
    }
}

/// Route + enqueue `payload` with resolved policy fields (CONCEPT:EG-277/278/279) —
/// the shared core behind [`publish_ex`] and the dead-letter republish. Per QUEUE it
/// resolves `expires_at` from the per-message TTL else the queue's `message_ttl_ms`
/// (so per-queue TTL is honored), then merges `priority` / `deliver_at` / `expires_at`
/// plus any `headers` (e.g. `x-death`) into the message node. Returns delivered count.
#[allow(clippy::too_many_arguments)]
fn publish_resolved(
    core: &GraphCore,
    exchange: &str,
    routing_key: &str,
    payload: &[u8],
    priority: i64,
    deliver_at: Option<u64>,
    ttl_ms: Option<u64>,
    now_ms: Option<u64>,
    headers: &serde_json::Map<String, serde_json::Value>,
) -> usize {
    let Some(kind) = load_exchange_kind(core, exchange) else {
        return 0;
    };
    let bindings = load_bindings(core, exchange);
    let queues = route(kind, &bindings, routing_key);
    if queues.is_empty() {
        return 0;
    }
    let payload_hex = hex_encode(payload);
    let mut delivered = 0usize;
    for q in &queues {
        let policy = load_queue_policy(core, q);
        let eff_ttl = ttl_ms.or(policy.message_ttl_ms);
        let expires_at = match (now_ms, eff_ttl) {
            (Some(n), Some(t)) => Some(n.saturating_add(t)),
            _ => None,
        };
        let mut extra = headers.clone();
        if priority != 0 {
            extra.insert("priority".into(), serde_json::Value::from(priority));
        }
        if let Some(da) = deliver_at {
            extra.insert("deliver_at".into(), serde_json::Value::from(da));
        }
        if let Some(ea) = expires_at {
            extra.insert("expires_at".into(), serde_json::Value::from(ea));
        }
        delivered += core.broker_enqueue_ex(
            std::slice::from_ref(q),
            exchange,
            routing_key,
            &payload_hex,
            &extra,
        );
    }
    delivered
}

/// Policy-carrying publish (CONCEPT:EG-277 TTL / EG-278 priority / EG-279 delay).
/// Stamps `priority` on each message; resolves `delay_ms`/`ttl_ms` against the
/// EXPLICIT `now_ms` to absolute `deliver_at`/`expires_at` etas (so replay is
/// deterministic). With `priority == 0` and every option `None`, the message node is
/// identical to a plain [`publish`]. Returns the delivered-queue count.
#[allow(clippy::too_many_arguments)]
pub fn publish_ex(
    core: &GraphCore,
    exchange: &str,
    routing_key: &str,
    payload: &[u8],
    priority: i64,
    delay_ms: Option<u64>,
    ttl_ms: Option<u64>,
    now_ms: Option<u64>,
) -> usize {
    let deliver_at = match (now_ms, delay_ms) {
        (Some(n), Some(d)) => Some(n.saturating_add(d)),
        _ => None,
    };
    publish_resolved(
        core,
        exchange,
        routing_key,
        payload,
        priority,
        deliver_at,
        ttl_ms,
        now_ms,
        &serde_json::Map::new(),
    )
}

// ── Message-node field accessors (single source of truth) ─────────────────

fn f_str<'a>(o: &'a serde_json::Map<String, serde_json::Value>, k: &str) -> &'a str {
    o.get(k).and_then(|v| v.as_str()).unwrap_or("")
}
fn f_i64(o: &serde_json::Map<String, serde_json::Value>, k: &str, dflt: i64) -> i64 {
    o.get(k).and_then(|v| v.as_i64()).unwrap_or(dflt)
}
fn f_u64(o: &serde_json::Map<String, serde_json::Value>, k: &str) -> Option<u64> {
    o.get(k).and_then(|v| v.as_u64())
}

/// A claim candidate distilled from a scanned message node.
struct Candidate {
    id: String,
    priority: i64,
    seq: i64,
    delivery_count: i64,
    status: String,
    lease_until: Option<u64>,
}

/// Total order for the claim pick (CONCEPT:EG-278): highest priority first, then
/// oldest seq (FIFO within a band), ties broken by id for determinism. Returns the
/// preferred of `a` (incumbent) and `b` (challenger).
fn prefer(a: Option<Candidate>, b: Candidate) -> Option<Candidate> {
    match a {
        None => Some(b),
        Some(cur) => {
            let b_wins = b.priority > cur.priority
                || (b.priority == cur.priority && b.seq < cur.seq)
                || (b.priority == cur.priority && b.seq == cur.seq && b.id < cur.id);
            if b_wins {
                Some(b)
            } else {
                Some(cur)
            }
        }
    }
}

/// Consume one message from `queue` for a consumer-group member (CONCEPT:EG-280
/// groups + QoS/prefetch), honoring EG-277 TTL / EG-278 priority / EG-279 delay.
///
/// Picks the highest-priority, oldest, DUE (`deliver_at <= now`), non-expired message
/// that is either `pending` or a `claimed` message whose visibility lease has expired
/// (EG-280 lease-return / redelivery). Enforces per-consumer `prefetch` (0 ⇒ unlimited)
/// by counting the consumer's in-flight (unexpired-lease) messages. Takes a visibility
/// lease of `lease_ms` (0 ⇒ none) and bumps `delivery_count`. Lazily dead-letters any
/// expired messages it steps over (CONCEPT:EG-277). Returns the claimed `(id, props)`
/// or `None` (nothing due / prefetch full). Deterministic in its explicit args.
pub fn broker_consume(
    core: &GraphCore,
    queue: &str,
    group: &str,
    consumer: &str,
    now_ms: u64,
    lease_ms: u64,
    prefetch: u32,
) -> Option<(String, serde_json::Value)> {
    let label = queue_msg_label(queue);
    // Bounded retry: dead-letter lazily-found expired messages then re-scan; retry a
    // lost CAS race. The pool only shrinks per iteration, so this terminates.
    for _ in 0..64 {
        let rows = core.get_nodes_by_label(&label, 0);
        let mut inflight: u32 = 0;
        let mut best: Option<Candidate> = None;
        let mut expired: Vec<String> = Vec::new();
        for (id, blob) in &rows {
            let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob) else {
                continue;
            };
            let Some(obj) = v.as_object() else { continue };
            let status = f_str(obj, "status");
            let lease_until = f_u64(obj, "lease_until");
            let mut reclaimable = false;
            match status {
                "claimed" => {
                    // A live (unexpired) lease is held by someone → not claimable.
                    let leased = lease_until.map(|l| l > now_ms).unwrap_or(true);
                    if leased {
                        if f_str(obj, "owner_consumer") == consumer {
                            inflight += 1;
                        }
                        continue;
                    }
                    // Lease expired (EG-280) → this message returns to the pool.
                    reclaimable = true;
                }
                "pending" => {}
                _ => continue, // done / unknown → not claimable
            }
            // EG-277: expired → collect for lazy dead-lettering, never deliver.
            if let Some(ea) = f_u64(obj, "expires_at") {
                if ea <= now_ms {
                    expired.push(id.clone());
                    continue;
                }
            }
            // EG-279: not yet due → skip (still non-claimable until its eta).
            if let Some(da) = f_u64(obj, "deliver_at") {
                if da > now_ms {
                    continue;
                }
            }
            let cand = Candidate {
                id: id.clone(),
                priority: f_i64(obj, "priority", 0),
                seq: f_i64(obj, "seq", i64::MAX),
                delivery_count: f_i64(obj, "delivery_count", 0),
                status: if reclaimable { "claimed".into() } else { "pending".into() },
                lease_until,
            };
            best = prefer(best, cand);
        }
        // Lazy dead-letter of expired messages (EG-277), id-sorted so DLQ seq order is
        // deterministic across replay, then re-scan.
        if !expired.is_empty() {
            expired.sort();
            for id in expired {
                if let Some(props) = core.get_node_properties(&id) {
                    if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(&props) {
                        dead_letter(core, queue, &id, &v, "expired", now_ms);
                    }
                }
            }
            continue;
        }
        // EG-280: per-consumer prefetch ceiling.
        if prefetch > 0 && inflight >= prefetch {
            return None;
        }
        let cand = best?;
        // Atomic claim: condition on the scanned status (+ the exact expired lease for a
        // reclaim, so a concurrent re-lease loses the race) then stamp ownership/lease.
        let mut conditions = serde_json::Map::new();
        conditions.insert("status".into(), serde_json::Value::String(cand.status.clone()));
        if cand.status == "claimed" {
            conditions.insert(
                "lease_until".into(),
                match cand.lease_until {
                    Some(l) => serde_json::Value::from(l),
                    None => serde_json::Value::Null,
                },
            );
        }
        let mut updates = serde_json::Map::new();
        updates.insert("status".into(), serde_json::Value::String("claimed".into()));
        updates.insert("owner_group".into(), serde_json::Value::String(group.into()));
        updates.insert(
            "owner_consumer".into(),
            serde_json::Value::String(consumer.into()),
        );
        updates.insert(
            "delivery_count".into(),
            serde_json::Value::from(cand.delivery_count + 1),
        );
        updates.insert("claimed_at".into(), serde_json::Value::from(now_ms));
        updates.insert(
            "lease_until".into(),
            if lease_ms > 0 {
                serde_json::Value::from(now_ms.saturating_add(lease_ms))
            } else {
                serde_json::Value::Null
            },
        );
        if core.compare_and_set_fields(&cand.id, &conditions, &updates) {
            let blob = core.get_node_properties(&cand.id)?;
            let val = rmp_serde::from_slice::<serde_json::Value>(&blob).ok()?;
            return Some((cand.id, val));
        }
        // CAS lost a race — re-scan and retry.
    }
    None
}

/// Acknowledge (remove) a claimed message, freeing its consumer's in-flight slot
/// (CONCEPT:EG-280). Returns whether the message existed.
pub fn broker_ack(core: &GraphCore, _queue: &str, node_id: &str) -> bool {
    let existed = core.has_node(node_id);
    if existed {
        core.remove_node(node_id.to_string());
    }
    existed
}

/// Reject a claimed message (CONCEPT:EG-276). If `requeue` and the message's
/// `delivery_count` is still under the queue's `max_delivery_count`, it returns to
/// claimable (`pending`, lease cleared); otherwise it is dead-lettered to the queue's
/// DL target (preserving `x-death`) or dropped when no DL exchange is set. Returns the
/// outcome (`requeued` / `dead-lettered` / `dropped` / `absent`).
pub fn broker_reject(
    core: &GraphCore,
    queue: &str,
    node_id: &str,
    requeue: bool,
    now_ms: u64,
) -> String {
    let Some(blob) = core.get_node_properties(node_id) else {
        return "absent".into();
    };
    let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(&blob) else {
        return "absent".into();
    };
    let Some(obj) = v.as_object() else {
        return "absent".into();
    };
    let dc = f_i64(obj, "delivery_count", 0);
    let policy = load_queue_policy(core, queue);
    let under_max = policy
        .max_delivery_count
        .map(|m| (dc as i128) < (m as i128))
        .unwrap_or(true);
    if requeue && under_max {
        // Return to the claimable pool (keep delivery_count — it counts the attempt).
        let mut updates = serde_json::Map::new();
        updates.insert("status".into(), serde_json::Value::String("pending".into()));
        updates.insert("lease_until".into(), serde_json::Value::Null);
        updates.insert("owner_consumer".into(), serde_json::Value::Null);
        updates.insert("owner_group".into(), serde_json::Value::Null);
        core.compare_and_set_fields(node_id, &serde_json::Map::new(), &updates);
        return "requeued".into();
    }
    let reason = if requeue {
        "max-delivery-exceeded"
    } else {
        "rejected"
    };
    let had_dlx = policy.dl_exchange.is_some();
    dead_letter(core, queue, node_id, &v, reason, now_ms);
    if had_dlx {
        "dead-lettered".into()
    } else {
        "dropped".into()
    }
}

/// Dead-letter one message (CONCEPT:EG-276): if the queue has a `dl_exchange`,
/// republish the payload to it (routing key = `dl_routing_key` else the original),
/// preserving priority and appending an `x-death` record (original queue/exchange/
/// routing-key/reason/count/time) — then remove the original node. With no DL exchange
/// the message is simply dropped. `props` is the message node's decoded properties.
fn dead_letter(
    core: &GraphCore,
    queue: &str,
    node_id: &str,
    props: &serde_json::Value,
    reason: &str,
    now_ms: u64,
) {
    if let Some(obj) = props.as_object() {
        let policy = load_queue_policy(core, queue);
        if let Some(dlx) = policy.dl_exchange.clone() {
            let orig_exchange = f_str(obj, "exchange").to_string();
            let orig_rk = f_str(obj, "routing_key").to_string();
            let dl_rk = policy.dl_routing_key.clone().unwrap_or(orig_rk.clone());
            let priority = f_i64(obj, "priority", 0);
            let payload = hex_decode(f_str(obj, "payload")).unwrap_or_default();
            // Accumulate x-death (RabbitMQ-style), newest first.
            let mut x_death = obj
                .get("x-death")
                .and_then(|v| v.as_array().cloned())
                .unwrap_or_default();
            x_death.insert(
                0,
                serde_json::json!({
                    "queue": queue,
                    "reason": reason,
                    "exchange": orig_exchange,
                    "routing-keys": [orig_rk],
                    "count": f_i64(obj, "delivery_count", 0),
                    "time": now_ms,
                }),
            );
            let mut headers = serde_json::Map::new();
            headers.insert("x-death".into(), serde_json::Value::Array(x_death));
            headers.insert(
                "x-first-death-queue".into(),
                serde_json::Value::String(queue.into()),
            );
            headers.insert(
                "x-first-death-reason".into(),
                serde_json::Value::String(reason.into()),
            );
            // Republish to the DL exchange (applies the DL target's own queue TTL).
            publish_resolved(
                core,
                &dlx,
                &dl_rk,
                &payload,
                priority,
                None,
                None,
                Some(now_ms),
                &headers,
            );
        }
    }
    core.remove_node(node_id.to_string());
}

/// Reaper sweep (CONCEPT:EG-277): across every known queue, dead-letter/drop messages
/// whose `expires_at` has passed and return messages whose visibility lease has expired
/// to claimable (EG-280). Called periodically by the scheduler with the current clock
/// (`now_ms` explicit → deterministic replay). Queues are discovered from their durable
/// seq-counter nodes. Returns the count of messages acted on.
pub fn sweep_expired(core: &GraphCore, now_ms: u64) -> usize {
    // Discover queues from their seq counters (every published/bound queue has one).
    let mut queues: Vec<String> = core
        .get_nodes_by_label(QUEUE_SEQ_TYPE, 0)
        .into_iter()
        .filter_map(|(_, blob)| {
            let v = rmp_serde::from_slice::<serde_json::Value>(&blob).ok()?;
            v.as_object()?.get("queue")?.as_str().map(String::from)
        })
        .collect();
    queues.sort();
    queues.dedup();
    let mut acted = 0usize;
    for q in queues {
        let label = queue_msg_label(&q);
        // Snapshot ids first (dead_letter mutates the graph), id-sorted for a
        // deterministic dead-letter order across replay.
        let mut rows = core.get_nodes_by_label(&label, 0);
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        for (id, blob) in rows {
            let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(&blob) else {
                continue;
            };
            let Some(obj) = v.as_object() else { continue };
            let status = f_str(obj, "status");
            if status != "pending" && status != "claimed" {
                continue;
            }
            let lease_expired = status == "claimed"
                && f_u64(obj, "lease_until").map(|l| l <= now_ms).unwrap_or(true);
            // EG-277: TTL expiry (a live-lease claimed message is left to its holder).
            if let Some(ea) = f_u64(obj, "expires_at") {
                if ea <= now_ms && (status == "pending" || lease_expired) {
                    dead_letter(core, &q, &id, &v, "expired", now_ms);
                    acted += 1;
                    continue;
                }
            }
            // EG-280: proactively return an expired lease to the claimable pool.
            if lease_expired {
                let mut conditions = serde_json::Map::new();
                conditions.insert("status".into(), serde_json::Value::String("claimed".into()));
                conditions.insert(
                    "lease_until".into(),
                    match f_u64(obj, "lease_until") {
                        Some(l) => serde_json::Value::from(l),
                        None => serde_json::Value::Null,
                    },
                );
                let mut updates = serde_json::Map::new();
                updates.insert("status".into(), serde_json::Value::String("pending".into()));
                updates.insert("lease_until".into(), serde_json::Value::Null);
                updates.insert("owner_consumer".into(), serde_json::Value::Null);
                if core.compare_and_set_fields(&id, &conditions, &updates) {
                    acted += 1;
                }
            }
        }
    }
    acted
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

    // ══════════════════════════════════════════════════════════════════════
    // Policy-extension test helpers + fixtures (CONCEPT:EG-276..280)
    // ══════════════════════════════════════════════════════════════════════

    /// Direct exchange `ex` → queue `q` (routing key `k`), the common test rig.
    fn rig() -> GraphCore {
        let core = GraphCore::new();
        declare_exchange(&core, "ex", ExchangeKind::Direct).unwrap();
        bind_queue(&core, "ex", "q", "k");
        core
    }

    /// Add a dead-letter exchange `dlx` → queue `dlq` and point `q`'s policy at it.
    fn with_dlq(core: &GraphCore, policy: QueuePolicy) {
        declare_exchange(core, "dlx", ExchangeKind::Fanout).unwrap();
        bind_queue(core, "dlx", "dlq", "");
        let policy = QueuePolicy {
            dl_exchange: Some("dlx".into()),
            ..policy
        };
        declare_queue(core, "q", &policy);
    }

    fn payload_of(props: &serde_json::Value) -> Vec<u8> {
        hex_decode(props.get("payload").and_then(|v| v.as_str()).unwrap()).unwrap()
    }

    /// Consume one message off the dead-letter queue (helper).
    fn consume_dlq(core: &GraphCore, now: u64) -> Option<(String, serde_json::Value)> {
        broker_consume(core, "dlq", "g", "c", now, 0, 0)
    }

    // ── CONCEPT:EG-276 dead-letter queues ─────────────────────────────────

    #[test]
    fn eg276_declare_queue_policy_roundtrips() {
        let core = GraphCore::new();
        let policy = QueuePolicy {
            dl_exchange: Some("dlx".into()),
            dl_routing_key: Some("dead".into()),
            max_delivery_count: Some(3),
            message_ttl_ms: Some(5000),
            queue_expiry_ms: Some(60000),
            max_priority: Some(9),
        };
        declare_queue(&core, "q", &policy);
        assert_eq!(load_queue_policy(&core, "q"), policy);
        // A queue with NO policy node → the all-None default (EG-275 behavior).
        assert_eq!(load_queue_policy(&core, "other"), QueuePolicy::default());
    }

    #[test]
    fn eg276_reject_no_requeue_dead_letters_with_xdeath() {
        let core = rig();
        with_dlq(&core, QueuePolicy::default());
        assert_eq!(publish(&core, "ex", "k", b"boom"), 1);
        let (id, _) = broker_consume(&core, "q", "g", "c", 1000, 0, 0).unwrap();
        // reject WITHOUT requeue → straight to the DLQ.
        assert_eq!(broker_reject(&core, "q", &id, false, 1000), "dead-lettered");
        // original message is gone from the source queue …
        assert!(broker_consume(&core, "q", "g", "c", 1001, 0, 0).is_none());
        // … and present on the DLQ with payload + x-death metadata preserved.
        let (_, dl) = consume_dlq(&core, 1002).expect("message must be on the DLQ");
        assert_eq!(payload_of(&dl), b"boom".to_vec());
        let xd = dl.get("x-death").and_then(|v| v.as_array()).unwrap();
        assert_eq!(xd.len(), 1);
        assert_eq!(xd[0].get("queue").and_then(|v| v.as_str()), Some("q"));
        assert_eq!(xd[0].get("reason").and_then(|v| v.as_str()), Some("rejected"));
    }

    #[test]
    fn eg276_reject_requeue_returns_then_max_deliveries_dead_letters() {
        let core = rig();
        with_dlq(
            &core,
            QueuePolicy {
                max_delivery_count: Some(2),
                ..QueuePolicy::default()
            },
        );
        assert_eq!(publish(&core, "ex", "k", b"retry"), 1);

        // Delivery #1 → reject/requeue (1 < 2) → back to claimable.
        let (id, p1) = broker_consume(&core, "q", "g", "c", 10, 0, 0).unwrap();
        assert_eq!(p1.get("delivery_count").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(broker_reject(&core, "q", &id, true, 10), "requeued");

        // Delivery #2 → reject/requeue but 2 is NOT < 2 → dead-lettered.
        let (id2, p2) = broker_consume(&core, "q", "g", "c", 20, 0, 0).unwrap();
        assert_eq!(id2, id, "same message redelivered");
        assert_eq!(p2.get("delivery_count").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(broker_reject(&core, "q", &id2, true, 20), "dead-lettered");

        assert!(broker_consume(&core, "q", "g", "c", 30, 0, 0).is_none());
        let (_, dl) = consume_dlq(&core, 40).expect("dead-lettered after max deliveries");
        assert_eq!(payload_of(&dl), b"retry".to_vec());
        let xd = dl.get("x-death").and_then(|v| v.as_array()).unwrap();
        assert_eq!(
            xd[0].get("reason").and_then(|v| v.as_str()),
            Some("max-delivery-exceeded")
        );
    }

    #[test]
    fn eg276_reject_with_no_dlq_drops() {
        let core = rig();
        assert_eq!(publish(&core, "ex", "k", b"x"), 1);
        let (id, _) = broker_consume(&core, "q", "g", "c", 1, 0, 0).unwrap();
        assert_eq!(broker_reject(&core, "q", &id, false, 1), "dropped");
        assert!(!core.has_node(&id));
    }

    // ── CONCEPT:EG-277 message TTL + expiry ───────────────────────────────

    #[test]
    fn eg277_expired_message_dead_lettered_lazily_on_claim() {
        let core = rig();
        with_dlq(&core, QueuePolicy::default());
        // TTL 100ms published at t=1000 → expires_at=1100.
        assert_eq!(publish_ex(&core, "ex", "k", b"stale", 0, None, Some(100), Some(1000)), 1);
        // Claiming at t=2000 finds it expired → lazily dead-letters it, delivers nothing.
        assert!(broker_consume(&core, "q", "g", "c", 2000, 0, 0).is_none());
        let (_, dl) = consume_dlq(&core, 2001).expect("expired message on DLQ");
        assert_eq!(payload_of(&dl), b"stale".to_vec());
        assert_eq!(
            dl.get("x-death").and_then(|v| v.as_array()).unwrap()[0]
                .get("reason")
                .and_then(|v| v.as_str()),
            Some("expired")
        );
    }

    #[test]
    fn eg277_unexpired_message_still_delivers() {
        let core = rig();
        assert_eq!(publish_ex(&core, "ex", "k", b"fresh", 0, None, Some(1000), Some(1000)), 1);
        // At t=1500 (< expires_at 2000) it is deliverable.
        let (_, p) = broker_consume(&core, "q", "g", "c", 1500, 0, 0).unwrap();
        assert_eq!(payload_of(&p), b"fresh".to_vec());
    }

    #[test]
    fn eg277_sweep_expired_dead_letters_and_returns_leases() {
        let core = rig();
        with_dlq(&core, QueuePolicy::default());
        // One TTL'd message (expires 1100) + one durable message.
        assert_eq!(publish_ex(&core, "ex", "k", b"ttl", 0, None, Some(100), Some(1000)), 1);
        assert_eq!(publish(&core, "ex", "k", b"keep"), 1);
        // Sweep at t=5000 acts on exactly the expired one.
        assert_eq!(sweep_expired(&core, 5000), 1);
        let (_, dl) = consume_dlq(&core, 5001).expect("swept message dead-lettered");
        assert_eq!(payload_of(&dl), b"ttl".to_vec());
        // The durable message survives and is still deliverable.
        let (_, p) = broker_consume(&core, "q", "g", "c", 5002, 0, 0).unwrap();
        assert_eq!(payload_of(&p), b"keep".to_vec());
    }

    #[test]
    fn eg277_sweep_returns_expired_lease_to_claimable() {
        let core = rig();
        assert_eq!(publish(&core, "ex", "k", b"m"), 1);
        // Claim with a 100ms lease at t=1000 (lease_until=1100).
        let (id, _) = broker_consume(&core, "q", "g", "c", 1000, 100, 0).unwrap();
        // Sweep past the lease returns it to claimable (count 1) …
        assert_eq!(sweep_expired(&core, 2000), 1);
        // … so a fresh consume redelivers it (delivery_count now 2).
        let (id2, p) = broker_consume(&core, "q", "g", "c", 2001, 0, 0).unwrap();
        assert_eq!(id2, id);
        assert_eq!(p.get("delivery_count").and_then(|v| v.as_i64()), Some(2));
    }

    // ── CONCEPT:EG-278 priority queues ────────────────────────────────────

    #[test]
    fn eg278_claim_is_priority_desc_then_fifo() {
        let core = rig();
        // seq0 p0, seq1 p5, seq2 p5, seq3 p1.
        assert_eq!(publish_ex(&core, "ex", "k", b"a", 0, None, None, None), 1);
        assert_eq!(publish_ex(&core, "ex", "k", b"b", 5, None, None, None), 1);
        assert_eq!(publish_ex(&core, "ex", "k", b"c", 5, None, None, None), 1);
        assert_eq!(publish_ex(&core, "ex", "k", b"d", 1, None, None, None), 1);
        let mut order = Vec::new();
        while let Some((id, p)) = broker_consume(&core, "q", "g", "c", 0, 0, 0) {
            order.push(payload_of(&p));
            broker_ack(&core, "q", &id);
        }
        // p5 band FIFO (b then c), then p1 (d), then p0 (a).
        assert_eq!(
            order,
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"a".to_vec()]
        );
    }

    #[test]
    fn eg278_no_priority_path_is_plain_fifo() {
        let core = rig();
        for m in [b"1".as_ref(), b"2", b"3"] {
            assert_eq!(publish(&core, "ex", "k", m), 1);
        }
        let mut order = Vec::new();
        while let Some((id, p)) = broker_consume(&core, "q", "g", "c", 0, 0, 0) {
            order.push(payload_of(&p));
            broker_ack(&core, "q", &id);
        }
        assert_eq!(order, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn eg278_publish_ex_defaults_match_plain_publish_node_shape() {
        // A default PublishEx (priority 0, no delay/ttl) must produce a message node
        // byte-identical to a plain EG-275 publish — the ADDITIVE guarantee.
        let plain = rig();
        assert_eq!(publish(&plain, "ex", "k", b"z"), 1);
        let ex = rig();
        assert_eq!(publish_ex(&ex, "ex", "k", b"z", 0, None, None, None), 1);
        let a = plain.get_node_properties(&message_node_id("q", 0)).unwrap();
        let b = ex.get_node_properties(&message_node_id("q", 0)).unwrap();
        let av: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
        let bv: serde_json::Value = rmp_serde::from_slice(&b).unwrap();
        assert_eq!(av, bv);
        // No policy fields leaked onto the node.
        for k in ["priority", "deliver_at", "expires_at"] {
            assert!(bv.get(k).is_none(), "unexpected field {k}");
        }
    }

    // ── CONCEPT:EG-279 delayed / scheduled delivery ───────────────────────

    #[test]
    fn eg279_delayed_message_held_until_eta() {
        let core = rig();
        // delay 500ms at t=1000 → deliver_at=1500.
        assert_eq!(publish_ex(&core, "ex", "k", b"later", 0, Some(500), None, Some(1000)), 1);
        // Before the eta: not claimable.
        assert!(broker_consume(&core, "q", "g", "c", 1200, 0, 0).is_none());
        // At/after the eta: delivered.
        let (_, p) = broker_consume(&core, "q", "g", "c", 1600, 0, 0).unwrap();
        assert_eq!(payload_of(&p), b"later".to_vec());
    }

    #[test]
    fn eg279_due_message_delivers_before_a_delayed_one() {
        let core = rig();
        assert_eq!(publish_ex(&core, "ex", "k", b"soon", 0, Some(1000), None, Some(0)), 1); // due at 1000
        assert_eq!(publish(&core, "ex", "k", b"now"), 1); // due immediately
        // At t=1: only the immediate one is due.
        let (id, p) = broker_consume(&core, "q", "g", "c", 1, 0, 0).unwrap();
        assert_eq!(payload_of(&p), b"now".to_vec());
        broker_ack(&core, "q", &id);
        assert!(broker_consume(&core, "q", "g", "c", 1, 0, 0).is_none()); // delayed still held
    }

    // ── CONCEPT:EG-280 consumer groups + QoS / prefetch ───────────────────

    #[test]
    fn eg280_prefetch_limits_inflight_and_ack_frees_a_slot() {
        let core = rig();
        for m in [b"m0".as_ref(), b"m1", b"m2"] {
            assert_eq!(publish(&core, "ex", "k", m), 1);
        }
        // c1 with prefetch=1 (lease held): claims one, then is blocked.
        let (id0, p0) = broker_consume(&core, "q", "g", "c1", 100, 0, 1).unwrap();
        assert_eq!(payload_of(&p0), b"m0".to_vec());
        assert!(
            broker_consume(&core, "q", "g", "c1", 100, 0, 1).is_none(),
            "prefetch=1 blocks a second in-flight claim"
        );
        // A different consumer shares the queue fairly and gets the next message.
        let (_id1, p1) = broker_consume(&core, "q", "g", "c2", 100, 0, 1).unwrap();
        assert_eq!(payload_of(&p1), b"m1".to_vec());
        // c1 acks → its slot frees → it can claim again.
        assert!(broker_ack(&core, "q", &id0));
        let (_id2, p2) = broker_consume(&core, "q", "g", "c1", 100, 0, 1).unwrap();
        assert_eq!(payload_of(&p2), b"m2".to_vec());
    }

    #[test]
    fn eg280_lease_expiry_redelivers_to_another_consumer() {
        let core = rig();
        assert_eq!(publish(&core, "ex", "k", b"once"), 1);
        // c1 claims with a 100ms lease at t=1000.
        let (id, p) = broker_consume(&core, "q", "g", "c1", 1000, 100, 0).unwrap();
        assert_eq!(p.get("delivery_count").and_then(|v| v.as_i64()), Some(1));
        // While the lease is live, nobody else can take it.
        assert!(broker_consume(&core, "q", "g", "c2", 1050, 100, 0).is_none());
        // After the lease expires it is redelivered to c2 (delivery_count bumps).
        let (id2, p2) = broker_consume(&core, "q", "g", "c2", 1200, 100, 0).unwrap();
        assert_eq!(id2, id);
        assert_eq!(p2.get("delivery_count").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(p2.get("owner_consumer").and_then(|v| v.as_str()), Some("c2"));
    }

    #[test]
    fn eg280_ack_removes_message() {
        let core = rig();
        assert_eq!(publish(&core, "ex", "k", b"x"), 1);
        let (id, _) = broker_consume(&core, "q", "g", "c", 1, 0, 0).unwrap();
        assert!(broker_ack(&core, "q", &id));
        assert!(!core.has_node(&id));
        assert!(!broker_ack(&core, "q", &id), "second ack is a no-op");
    }
}
