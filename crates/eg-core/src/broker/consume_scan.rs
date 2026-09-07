//! Picking the next deliverable message for a consumer
//! (CONCEPT:EG-KG.compute.groups-qos-prefetch-honoring).
//!
//! One advisory pass over a queue's message nodes classifies every row for the calling
//! consumer — another consumer's live lease, this consumer's own in-flight message, a
//! TTL-expired row to dead-letter, a not-yet-due row, or a claimable one — and returns the
//! preferred candidate together with the counts and the expired ids the caller needs. The
//! pass is advisory only: the core revalidates the winner inside one topology transaction,
//! so nothing here can publish a stale delivery.

use super::*;

/// A claim candidate distilled from a scanned message node.
pub(super) struct Candidate {
    pub(super) id: String,
    priority: i64,
    seq: i64,
    pub(super) status: String,
    pub(super) lease_until: Option<u64>,
}

/// Total order for the claim pick (CONCEPT:EG-KG.compute.priority-queues): highest priority first, then
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

/// One advisory pass over a queue's messages for a consumer.
pub(super) struct QueueScan {
    /// How many of the queue's messages this consumer already holds under a live lease.
    pub(super) inflight: u32,
    /// The preferred claim candidate: highest priority, then oldest seq, then lowest id.
    pub(super) best: Option<Candidate>,
    /// TTL-expired messages stepped over, to be dead-lettered before the next pass.
    pub(super) expired: Vec<String>,
}

/// Scan every message of `label`, classifying each for `consumer` at `now_ms`.
pub(super) fn scan_queue(core: &GraphCore, label: &str, consumer: &str, now_ms: u64) -> QueueScan {
    let mut scan = QueueScan {
        inflight: 0,
        best: None,
        expired: Vec::new(),
    };
    for (id, blob) in &core.get_nodes_by_label(label, 0) {
        let Ok(v) = decode_property(blob) else {
            continue;
        };
        let Some(obj) = v.as_object() else { continue };
        match claimability(obj, consumer, now_ms) {
            Claimability::Skip => {}
            Claimability::InFlight => scan.inflight += 1,
            Claimability::Expired => scan.expired.push(id.clone()),
            Claimability::Claimable { reclaimed } => {
                scan.best = prefer(scan.best, scan_candidate(id, obj, reclaimed));
            }
        }
    }
    scan
}

/// What one scanned queue message is to the consuming call.
enum Claimability {
    /// Not claimable: done/unknown status, another consumer's live lease, or not yet due
    /// (EG-279).
    Skip,
    /// Held by THIS consumer under a live lease — it counts against the prefetch ceiling.
    InFlight,
    /// TTL-expired (EG-277): never deliver it; dead-letter it lazily instead.
    Expired,
    /// Claimable now — `reclaimed` when it is a `claimed` message whose visibility lease
    /// expired (EG-280 lease-return) rather than a plain `pending` one.
    Claimable { reclaimed: bool },
}

/// Classify one queue message for `consumer` at `now_ms`.
fn claimability(
    obj: &serde_json::Map<String, serde_json::Value>,
    consumer: &str,
    now_ms: u64,
) -> Claimability {
    let reclaimed = match f_str(obj, "status") {
        "claimed" => {
            // A live (unexpired) lease is held by someone → not claimable.
            let leased = f_u64(obj, "lease_until")
                .map(|l| l > now_ms)
                .unwrap_or(true);
            if leased {
                return if f_str(obj, "owner_consumer") == consumer {
                    Claimability::InFlight
                } else {
                    Claimability::Skip
                };
            }
            true // lease expired (EG-280) → this message returns to the pool
        }
        "pending" => false,
        _ => return Claimability::Skip, // done / unknown → not claimable
    };
    if f_u64(obj, "expires_at").is_some_and(|ea| ea <= now_ms) {
        return Claimability::Expired;
    }
    if f_u64(obj, "deliver_at").is_some_and(|da| da > now_ms) {
        return Claimability::Skip;
    }
    Claimability::Claimable { reclaimed }
}

/// The scan candidate for a claimable message.
fn scan_candidate(
    id: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    reclaimed: bool,
) -> Candidate {
    Candidate {
        id: id.to_string(),
        priority: f_i64(obj, "priority", 0),
        seq: f_i64(obj, "seq", i64::MAX),
        status: if reclaimed { "claimed" } else { "pending" }.into(),
        lease_until: f_u64(obj, "lease_until"),
    }
}

/// Dead-letter every TTL-expired message the scan stepped over (EG-277), id-sorted so DLQ
/// seq order is deterministic across replay.
pub(super) fn dead_letter_expired(
    core: &GraphCore,
    queue: &str,
    mut expired: Vec<String>,
    now_ms: u64,
) {
    expired.sort();
    for id in expired {
        if let Some(props) = core.get_node_properties(&id) {
            if let Ok(v) = decode_property(&props) {
                dead_letter(core, queue, &id, &v, "expired", now_ms);
            }
        }
    }
}
