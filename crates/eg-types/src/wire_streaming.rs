//! Streaming, CDC, continuous-query, and trigger wire DTOs.

#[cfg(feature = "streaming")]
use serde::{Deserialize, Serialize};

// ── Streaming / CDC / continuous queries / watch / triggers (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ──
// PURE-data wire DTOs for the reactive surface. They are produced/consumed by the
// `streaming` handler over the existing one-Response-per-Request transport (cursor /
// long-poll, NOT a side-channel), so the enum stays free of any server type.

/// The kind of a captured change. Mirrors the durable-mutation `Method` set the CDC
/// feed records (the ledger's `is_durable_mutation` family), flattened to the unit a
/// consumer reasons about (node add/remove/update, edge add/remove).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CdcKind {
    AddNode,
    RemoveNode,
    UpdateNode,
    AddEdge,
    RemoveEdge,
}

/// One ordered change in a graph's CDC feed (CONCEPT:EG-KG.query.streaming-cdc-subscriptions). `seq` is the per-graph
/// monotonic cursor: a consumer tails with `CdcRead { from_seq }` and re-reads from a
/// later `seq` to skip what it has already seen. `before`/`after` carry the affected
/// node/edge property blob (MessagePack) pre- and post-mutation; `None` means absent
/// (an add has no `before`, a remove no `after`).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CdcEvent {
    pub seq: u64,
    pub graph: String,
    pub kind: CdcKind,
    /// The node id for node changes / the edge source for edge changes.
    pub node_id: String,
    /// The edge target (empty for node changes).
    #[serde(default)]
    pub target_id: String,
    /// The change's `type`/label property after the mutation (for label-filtered
    /// watch + triggers); best-effort decoded from `after` (else `before`).
    #[serde(default)]
    pub label: String,
    #[serde(default, with = "serde_bytes")]
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    pub before: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    pub after: Vec<u8>,
    /// True when `before` carried a value (distinguishes an empty blob from absent).
    #[serde(default)]
    pub had_before: bool,
    /// True when `after` carried a value.
    #[serde(default)]
    pub had_after: bool,
}

/// Spec for a registered continuous query (CONCEPT:EG-KG.query.streaming-cdc-subscriptions), incrementally maintained
/// as CDC changes arrive. One graph, one label filter, one aggregate. Deliberately
/// SIMPLE — a counting/sum/filter view that updates on delta rather than re-running.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuousQuerySpec {
    /// The graph whose CDC feed drives this view.
    pub graph: String,
    /// Only changes whose label equals this maintain the view (empty ⇒ all nodes).
    #[serde(default)]
    pub label: String,
    /// The aggregate maintained: "count" (live node count for the label) or
    /// "sum:<field>" (running sum of a numeric node property).
    pub agg: ContinuousAgg,
}

/// The incremental aggregate a continuous query maintains.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ContinuousAgg {
    /// Live count of matching nodes (add +1, remove -1).
    Count,
    /// Running sum of a numeric node property (delta = new - old on update).
    Sum { field: String },
}

/// Current result of a continuous query — the incrementally-maintained value plus the
/// CDC `seq` it reflects (so a reader knows how current it is).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ContinuousQueryResult {
    pub name: String,
    /// The aggregate value (count, or sum).
    pub value: f64,
    /// The CDC seq this result has folded in up to (the watermark).
    pub through_seq: u64,
}

/// A batch returned by a `Watch` long-poll (CONCEPT:EG-KG.query.wire-codec): the matching changes
/// since the client's cursor plus the next cursor to resume from.
///
/// B-8 (2026-08-13): `Watch` tails the SAME bounded, explicitly-ephemeral
/// `CdcHub` ring `CdcRead` does (`src/server/cdc.rs`'s module doc) via the
/// identical `from_seq` cursor, so it can fall off the back of that ring in
/// the identical two ways `CdcReadResult` documents (within-epoch trim, or
/// an epoch reset from a process restart / `ClearGraph`/`FromMsgpack`/
/// `Reconcile`) — an empty `events` used to be indistinguishable from "caught
/// up". `gap: true` makes that explicit (`events` is then always empty);
/// `watermark`/`head_seq`/`epoch` carry the same meaning as on
/// `CdcReadResult` so a caller resuming `Watch` can detect the gap exactly
/// the same way.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WatchBatch {
    pub events: Vec<CdcEvent>,
    /// The cursor to pass as `from_seq` next time (one past the last delivered seq).
    pub next_seq: u64,
    /// True when `from_seq` could not be served contiguously (see the struct
    /// doc). `events` is always empty when `gap` is true.
    #[serde(default)]
    pub gap: bool,
    /// Oldest seq this epoch's ring can currently vouch for. See
    /// `CdcReadResult::watermark` — same field, same ephemeral-ring caveat.
    #[serde(default)]
    pub watermark: u64,
    /// The current head seq (next to be assigned) for this graph in this epoch.
    #[serde(default)]
    pub head_seq: u64,
    /// This hub instance's process-lifetime epoch id. See `CdcReadResult::epoch`.
    #[serde(default)]
    pub epoch: u64,
}

/// One trigger registration (CONCEPT:EG-KG.query.wire-codec). When a CDC change in `graph` matches
/// `label` + `op`, the trigger fires its `action` (an opaque MessagePack payload the
/// consumer interprets — e.g. a notification topic / a webhook spec). Fired actions
/// are recorded in a per-graph fired log a client polls with `FiredTriggers`.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TriggerInfo {
    pub name: String,
    pub graph: String,
    #[serde(default)]
    pub label: String,
    /// "add" | "remove" | "update" | "any" — the change kind that fires it.
    pub op: String,
    /// How many times this trigger has fired.
    pub fire_count: u64,
}

/// A recorded trigger firing (CONCEPT:EG-KG.query.wire-codec). Returned by `FiredTriggers` so a
/// reaction consumer pulls the action payload + the change that fired it.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FiredAction {
    /// Per-graph monotonic fired-id (the cursor for `FiredTriggers`).
    pub fire_seq: u64,
    pub trigger: String,
    pub graph: String,
    /// The CDC seq of the change that fired the trigger.
    pub change_seq: u64,
    pub node_id: String,
    /// The opaque action payload registered with the trigger (MessagePack).
    #[serde(default, with = "serde_bytes")]
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    pub action: Vec<u8>,
}

/// Materialized result of a `Method::CdcRead` call (B-8, 2026-08-13). Same
/// defect class as `GetLedger` (BUG A1): `CdcHub` (`src/server/cdc.rs`) backs
/// `CdcRead`/`Watch`/`FiredTriggers` with a bounded, per-graph, PURELY
/// IN-MEMORY ring (default 65,536 entries, `EPISTEMIC_GRAPH_CDC_RING` to
/// override) — deliberately EPHEMERAL, not durable (see that module's own
/// doc for the reasoning and why durability is a separate, out-of-scope,
/// storage-format decision). A caller that persists a `from_seq` cursor and
/// resumes tailing can fall off the back of that ring in two distinct ways
/// that both used to look exactly like "nothing happened":
///
///   1. **within-epoch trim** — the ring exceeded its cap and dropped the
///      oldest entries past `from_seq` (same process, same epoch).
///   2. **epoch reset** — the process restarted (or the graph's feed was
///      rewound by `ClearGraph`/`FromMsgpack`/`Reconcile`), so the feed's
///      seq numbering restarted from 0 in a FRESH epoch. This is the more
///      dangerous case: `from_seq` can be numerically "in range" of the new
///      epoch's `[watermark, head_seq]` window WITHOUT naming the same
///      events at all — a purely numeric `from_seq <= head_seq` check
///      cannot tell the two epochs apart. `epoch` exists for exactly this:
///      it is a fresh id minted once when the hub is constructed
///      (`CdcHub::new()`), stable for the process's life. A caller that
///      persists `(epoch, from_seq)` alongside its cursor and observes a
///      DIFFERENT `epoch` on a later read has PROOF the feed restarted
///      under it, even when the server's own numeric check above could not
///      catch it.
///
/// `gap: true` covers case 1 (server-detectable) explicitly — `events` is
/// always empty in that case, and `watermark`/`head_seq` are the current
/// values so the caller can re-seed (`watermark` to replay everything still
/// retained, `head_seq` to resume from "now" and accept the gap). `gap:
/// false` means `events` is a complete, contiguous read from `from_seq`
/// WITHIN the returned `epoch` — the caller is still responsible for
/// comparing `epoch` across resumed reads to catch case 2.
#[cfg(feature = "streaming")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CdcReadResult {
    pub events: Vec<CdcEvent>,
    /// True when `from_seq` fell behind the retained ring window in THIS
    /// epoch. `events` is always empty when `gap` is true.
    pub gap: bool,
    /// Oldest seq this epoch's ring can currently vouch for (0 if nothing
    /// has been trimmed yet in this epoch, INCLUDING right after a restart —
    /// which is exactly why `epoch` exists alongside it).
    pub watermark: u64,
    /// The current head seq (next to be assigned) for this graph in this epoch.
    pub head_seq: u64,
    /// This hub instance's process-lifetime epoch id. See the struct doc.
    pub epoch: u64,
}

#[cfg(feature = "streaming")]
impl CdcReadResult {
    /// `from_seq` was served contiguously from the retained ring.
    pub fn ok(events: Vec<CdcEvent>, watermark: u64, head_seq: u64, epoch: u64) -> Self {
        Self {
            events,
            gap: false,
            watermark,
            head_seq,
            epoch,
        }
    }

    /// `from_seq` could not be served contiguously (within-epoch trim past
    /// the ring's retained window). `events` is always empty.
    pub fn gap(watermark: u64, head_seq: u64, epoch: u64) -> Self {
        Self {
            events: Vec::new(),
            gap: true,
            watermark,
            head_seq,
            epoch,
        }
    }
}

/// Materialized result of a `Method::FiredTriggers` call (B-8 follow-up,
/// 2026-08-13). The fired-trigger log is a SECOND bounded in-memory ring on
/// the same per-graph `GraphFeed` the CDC ring lives on (`src/server/cdc.rs`),
/// with the identical ephemerality and the identical two gap shapes
/// `CdcReadResult` documents — within-epoch trim and epoch reset — over its
/// own `fire_seq` cursor rather than the CDC `seq` cursor. See
/// `CdcReadResult`'s doc for the full reasoning; the fields here mean exactly
/// the same thing, scoped to the fired-trigger log instead of the change feed.
#[cfg(feature = "streaming")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FiredTriggersResult {
    pub fired: Vec<FiredAction>,
    pub gap: bool,
    pub watermark: u64,
    pub head_seq: u64,
    pub epoch: u64,
}

#[cfg(feature = "streaming")]
impl FiredTriggersResult {
    pub fn ok(fired: Vec<FiredAction>, watermark: u64, head_seq: u64, epoch: u64) -> Self {
        Self {
            fired,
            gap: false,
            watermark,
            head_seq,
            epoch,
        }
    }

    pub fn gap(watermark: u64, head_seq: u64, epoch: u64) -> Self {
        Self {
            fired: Vec::new(),
            gap: true,
            watermark,
            head_seq,
            epoch,
        }
    }
}
