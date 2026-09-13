use super::*;

/// The error `add_edge`/`add_edge_no_ledger` return when an endpoint id is
/// absent from THIS graph's own `node_map`.
///
/// A `GraphCore`/`GraphView` has no notion of "which graph am I" — it is a
/// bare topology keyed only by node id, with no graph name or cross-graph
/// reference of any kind. Every request is dispatched to exactly one graph's
/// core (resolved by graph name at the registry, one layer above this
/// module), and each graph keeps its own independent node-id namespace, so
/// there is no code path by which an id genuinely belonging to a DIFFERENT
/// graph could ever be looked up here. "Not found" therefore always means
/// one of: a typo/stale id, a node not yet added on this write path, OR —
/// the case this message exists to head off — a caller trying to link two
/// endpoints that live in different graphs, which this API structurally
/// cannot do (an edge cannot span graphs; add both endpoints to the same
/// graph, or use a cross-graph query/reference mechanism instead of a native
/// edge). Without this framing the bare "node not found" reads as a missing-
/// data bug and sends readers hunting for a deleted/never-written node,
/// exactly the confusion this wording is meant to prevent.
pub(super) fn edge_endpoint_not_found(role: &str, id: &str) -> String {
    format!(
        "{role} node '{id}' not found in this graph. Edges cannot span \
         graphs — each graph has its own independent node namespace, so \
         `add_edge` only ever looks up '{id}' within the one graph this \
         call is targeting. If '{id}' exists, it is either a typo/stale id \
         or it belongs to a DIFFERENT graph than the one you're writing to; \
         add both edge endpoints to the same graph, or use a cross-graph \
         query/reference instead of a native edge.",
    )
}

/// Streams a byte slice as lowercase hex DIRECTLY into a formatter (CONCEPT:AU-KG.backend.b-auto-size).
///
/// `format!("…|{}", hex::encode(&blob))` allocated the 2·N-byte hex String TWICE — once
/// for `hex::encode`'s return, then again as `format!` copied it into the final buffer —
/// all while the topology write guard is held. For a large property blob that transient
/// double-allocation is a leading Pi memory + lock-hold driver. Using this `Display`
/// adapter, `format!` writes the hex digits straight into its single output buffer with
/// no intermediate String, so the ledger line is built with ONE allocation. The emitted
/// text is byte-identical to `hex::encode` (lowercase, 2 chars/byte), so the on-disk
/// ledger format and every consumer (audit mirror, redb `ledger` table, snapshot replay)
/// are unchanged.
pub(super) struct HexLedger<'a>(pub(super) &'a [u8]);

impl std::fmt::Display for HexLedger<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for &b in self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

/// Cap on `GraphCore::ledger`'s length (BUG A1 follow-up, 2026-08-12): the
/// in-memory mutation ledger is an ephemeral, bounded ring, NOT a durable
/// change log. Exceeding this drops the oldest half — see
/// `GraphTxn::push_ledger`'s doc for the full reasoning and
/// `GraphCore::ledger_watermark` for how a caller detects it happened.
pub(super) const LEDGER_CAP: usize = 100_000;
/// How many of the oldest entries `GraphTxn::push_ledger` drops in one trim,
/// once `LEDGER_CAP` is exceeded.
pub(super) const LEDGER_TRIM: usize = 50_000;

/// Shared implementation behind `GraphTxn::push_ledger` — the staged-
/// transaction writer every production mutation actually goes through
/// (`add_node`/`remove_node`/`add_edge`/`remove_edge`/...). Split out as its
/// own free function (rather than inlined into that one method) so a future
/// second caller can share the exact same drop-oldest-half + watermark-
/// accounting policy without duplicating it. See
/// `GraphCore::ledger_dropped_total`'s field doc for the full BUG A1
/// follow-up reasoning.
pub(super) fn push_ledger_impl(
    ledger: &Mutex<Vec<String>>,
    dropped_total: &std::sync::atomic::AtomicU64,
    entry: String,
) {
    let mut ledger = ledger.lock();
    ledger.push(entry);
    if ledger.len() > LEDGER_CAP {
        let dropped = ledger.drain(0..LEDGER_TRIM).count() as u64;
        let total_dropped =
            dropped_total.fetch_add(dropped, std::sync::atomic::Ordering::Relaxed) + dropped;
        let retained = ledger.len();
        drop(ledger);
        tracing::warn!(
            dropped,
            total_dropped,
            retained,
            "graph mutation ledger exceeded its {LEDGER_CAP}-entry cap and dropped its \
             oldest {LEDGER_TRIM} entries -- GetLedger watermark advanced to {total_dropped}; \
             durable state is unaffected, only the in-memory audit ledger lost this history"
        );
    }
}
