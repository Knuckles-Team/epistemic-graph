use super::*;

// ══════════════════════════════════════════════════════════════════════════
// CONCEPT:EG-KG.ingest.broker-reject-publish idempotent producer (effectively-once publish) — ADDITIVE over
// EG-275/276..284. A publish MAY carry a `(producer_id, seq)` idempotency stamp;
// the broker keeps a durable per-producer monotonic high-water mark on the SAME
// control graph and DROPS a re-published `(producer_id, seq)` it has already seen
// (seq at/under the mark), so a publisher that retries after an ambiguous
// publisher-confirm gets effectively-once delivery instead of a duplicate. A
// publish WITHOUT a producer-id behaves EXACTLY as today (at-least-once) — no
// producer node is touched, no message shape changes.
//
// Determinism/atomicity: the dedup decision + high-water-mark bump run under ONE
// GraphCore write guard and derive purely from the producer node's current state
// (the caller supplies `producer_id`/`seq`; no server clock / RNG), so a WAL/Raft
// replay of `Method::PublishIdempotent` reproduces byte-identical state — the same
// discipline EG-275..284 follow.
// ══════════════════════════════════════════════════════════════════════════

/// Type carried by a producer's durable dedup high-water-mark node (CONCEPT:EG-KG.ingest.broker-reject-publish).
pub const PRODUCER_SEQ_TYPE: &str = "BrokerProducerSeq";

/// Node id for a producer's durable dedup state (CONCEPT:EG-KG.ingest.broker-reject-publish) — the per-producer
/// monotonic `last_seq` high-water mark the broker dedups against. The `producer_id`
/// is caller-chosen (a stable publisher identity), so the id is deterministic.
pub fn producer_seq_node_id(producer_id: &str) -> String {
    format!("broker:producer:{producer_id}")
}

pub use eg_types::messaging_wire::IdempotentPublish;

/// Inputs for an idempotent publish, grouped so policy and producer identity
/// cannot be swapped at a positional call boundary.
pub struct IdempotentPublishRequest<'a> {
    pub core: &'a GraphCore,
    pub exchange: &'a str,
    pub routing_key: &'a str,
    pub payload: &'a [u8],
    pub producer_id: Option<&'a str>,
    pub seq: i64,
    pub priority: i64,
    pub delay_ms: Option<u64>,
    pub ttl_ms: Option<u64>,
    pub now_ms: Option<u64>,
}

/// Publish with an optional producer sequence, returning whether the exchange
/// confirmed, deduplicated, and routed the message.
pub fn publish_idempotent(request: IdempotentPublishRequest<'_>) -> IdempotentPublish {
    let IdempotentPublishRequest {
        core,
        exchange,
        routing_key,
        payload,
        producer_id,
        seq,
        priority,
        delay_ms,
        ttl_ms,
        now_ms,
    } = request;
    let confirmed = load_exchange_kind(core, exchange).is_some();
    // No producer-id ⇒ the unchanged at-least-once path (no dedup, no producer node).
    let Some(pid) = producer_id.filter(|p| !p.is_empty()) else {
        let delivered = if confirmed {
            publish_ex(
                core,
                exchange,
                routing_key,
                payload,
                priority,
                delay_ms,
                ttl_ms,
                now_ms,
            )
        } else {
            0
        };
        return IdempotentPublish {
            confirmed,
            duplicate: false,
            delivered,
        };
    };
    // Unknown exchange ⇒ nack WITHOUT recording the seq (nothing was accepted, so a
    // retry once the exchange exists must still be delivered).
    if !confirmed {
        return IdempotentPublish {
            confirmed: false,
            duplicate: false,
            delivered: 0,
        };
    }
    // Dedup: a (producer_id, seq) already at/under the high-water mark is a duplicate.
    let is_new = core.broker_producer_check_and_record(&producer_seq_node_id(pid), seq);
    if !is_new {
        // Effectively-once: confirm the duplicate but DO NOT re-enqueue it.
        return IdempotentPublish {
            confirmed: true,
            duplicate: true,
            delivered: 0,
        };
    }
    let delivered = publish_ex(
        core,
        exchange,
        routing_key,
        payload,
        priority,
        delay_ms,
        ttl_ms,
        now_ms,
    );
    IdempotentPublish {
        confirmed: true,
        duplicate: false,
        delivered,
    }
}
