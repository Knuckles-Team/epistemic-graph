//! [`BeliefGraph`] — the compact support/contradiction/attack projection the
//! propagation walk runs over, plus the one adapter that builds it from an engine
//! [`GraphView`] snapshot.
//!
//! Isolating the blob-decode here keeps the propagation algorithm ([`crate::propagate`])
//! pure and unit-testable against hand-built fixtures ([`BeliefGraph::from_parts`]),
//! while the `GraphView` decode reuses the exact `rmp_serde -> serde_json::Value`
//! pattern the planner's FILTER leg already uses.

use std::collections::HashMap;

use eg_core::graph::GraphView;

use crate::model::{classify_relationship, BiTemporalRecord, EdgeKind, TimeAxis};

/// A read-only projection of a graph's epistemic topology: each node's stored
/// confidence prior, and the support/contradiction/attack edges INCOMING to each node.
#[derive(Clone, Debug, Default)]
pub struct BeliefGraph {
    /// node id → stored `NodeData.confidence` prior.
    pub priors: HashMap<String, f64>,
    /// target id → [(source id, kind)] — the evidence bearing on the target's belief.
    pub in_edges: HashMap<String, Vec<(String, EdgeKind)>>,
    /// The bitemporal instant this projection was pinned at (set when the caller
    /// composed an `AS OF` filter before building it); `None` = as of now.
    pub as_of: Option<(TimeAxis, u64)>,
    /// node id → its RLS [`RowVisibility`](eg_core::isolation::RowVisibility)
    /// (CONCEPT:EG-KG.sharding.row-level-security), decoded from the SAME property blob
    /// `filter_view` reads on every other read path. Consulted ONLY by
    /// [`crate::redact::explain_belief_redacted`] (EPI-P3-4) to decide, per proof-tree
    /// node, whether the requesting actor may see that node's identity. A node absent
    /// from this map (no property blob at all) is untagged and therefore hidden by
    /// the same default-deny rule `filter_view` applies. Behind `epistemic-redaction` so a plain
    /// build carries no extra dependency or per-node decode cost.
    #[cfg(feature = "epistemic-redaction")]
    pub node_visibility: HashMap<String, eg_core::isolation::RowVisibility>,
    /// node id → its stored bitemporal window (EPI-P3-5, CONCEPT:EG-KG.epistemic.bitemporal-reasoning).
    /// Populated from the SAME `valid_from`/`valid_until`/`tx_from`/`tx_to` property
    /// keys `eg-plan::exec::live_at` reads for `Op::AsOf` — a node absent from this map
    /// carries no bitemporal metadata at all (treated as always-live, mirroring that
    /// same fn's `from.unwrap_or(0)` convention). This is what makes a belief genuinely
    /// **bitemporal-queryable** ([`Self::at_instant`]): the graph itself can be narrowed
    /// to what was live on BOTH axes at once, not just a single `AS OF` axis tagged onto
    /// the output afterward.
    pub temporal: HashMap<String, BiTemporalRecord>,
    /// The `(valid_ts, tx_ts)` instant [`Self::at_instant`] narrowed this projection to,
    /// if any; `None` = unnarrowed (as of now on both axes).
    pub bitemporal_pin: Option<(Option<u64>, Option<u64>)>,
    /// node id → its located multimodal evidence locus, CONCEPT:EG-X1 (`crate::evidence`).
    /// Decoded from the same per-node property blob as `priors`/`temporal` above.
    /// A node is present only when its `evidence_locus` property is a complete,
    /// validated governed locus.
    #[cfg(feature = "evidence-graph")]
    pub evidence_loci: HashMap<String, eg_modality::EvidenceLocus>,
}

impl BeliefGraph {
    /// Build from a `GraphView` snapshot. Decodes each node's `confidence` and each
    /// epistemic edge's canonical `relationship` from the msgpack property blobs; neutral
    /// (non-support/contradict/attack) edges are ignored.
    pub fn from_graph_view(view: &GraphView) -> Self {
        let mut priors = HashMap::with_capacity(view.node_properties.len());
        let mut temporal = HashMap::with_capacity(view.node_properties.len());
        #[cfg(feature = "evidence-graph")]
        let mut evidence_loci = HashMap::with_capacity(view.node_properties.len());
        for (id, blob) in &view.node_properties {
            let parsed = eg_types::msgpack::decode_property_value(blob).ok();
            let confidence = parsed
                .as_ref()
                .and_then(|v| v.get("confidence").and_then(|c| c.as_f64()))
                .unwrap_or(1.0);
            priors.insert(id.clone(), confidence);

            // Same blob, same decode pass — read the bitemporal window alongside
            // confidence (EPI-P3-5) so `at_instant` can narrow the graph on both axes
            // without a second scan.
            if let Some(v) = &parsed {
                let rec = BiTemporalRecord {
                    valid_from: v.get("valid_from").and_then(|x| x.as_u64()),
                    valid_until: v.get("valid_until").and_then(|x| x.as_u64()),
                    tx_from: v.get("tx_from").and_then(|x| x.as_u64()),
                    tx_to: v.get("tx_to").and_then(|x| x.as_u64()),
                };
                if rec != BiTemporalRecord::default() {
                    temporal.insert(id.clone(), rec);
                }
            }

            // Same blob, same decode pass: accept only the complete governed locus.
            #[cfg(feature = "evidence-graph")]
            if let Some(locus) = crate::evidence::decode_locus(parsed.as_ref()) {
                evidence_loci.insert(id.clone(), locus);
            }
        }

        let mut in_edges: HashMap<String, Vec<(String, EdgeKind)>> = HashMap::new();
        for ((source, target), blobs) in &view.edge_properties {
            for blob in blobs {
                let Some(kind) = eg_types::msgpack::decode_property_value(blob)
                    .ok()
                    .and_then(|v| {
                        v.get("relationship")
                            .and_then(|r| r.as_str())
                            .and_then(classify_relationship)
                    })
                else {
                    continue;
                };
                in_edges
                    .entry(target.clone())
                    .or_default()
                    .push((source.clone(), kind));
            }
        }

        // BUG A3 (2026-08-12): TBox membership is DERIVED from this
        // snapshot's `view.schema_node_ids` (populated from
        // `GraphCore::schema_refs` at snapshot time), never decoded from the
        // blob (`row_visibility`'s own `.schema` is always `false` now — see
        // that function's doc). Mirrors `IsolationLayer::filter_view`'s
        // identical derivation for the ordinary graph-read path.
        #[cfg(feature = "epistemic-redaction")]
        let node_visibility = view
            .node_properties
            .iter()
            .map(|(id, blob)| {
                let mut vis = eg_core::isolation::row_visibility(blob);
                vis.schema = view.schema_node_ids.contains(id);
                (id.clone(), vis)
            })
            .collect();

        BeliefGraph {
            priors,
            in_edges,
            as_of: None,
            #[cfg(feature = "epistemic-redaction")]
            node_visibility,
            temporal,
            bitemporal_pin: None,
            #[cfg(feature = "evidence-graph")]
            evidence_loci,
        }
    }

    /// Pin this projection to a bitemporal instant (the caller applied an `AS OF`
    /// filter upstream). Recorded on the resulting [`crate::BeliefState`].
    pub fn pinned_at(mut self, axis: TimeAxis, ts: u64) -> Self {
        self.as_of = Some((axis, ts));
        self
    }

    /// Test/utility constructor from explicit `(id, prior)` nodes and
    /// `(source, target, kind)` edges — no `GraphView` needed.
    pub fn from_parts<'a, N, E>(nodes: N, edges: E) -> Self
    where
        N: IntoIterator<Item = (&'a str, f64)>,
        E: IntoIterator<Item = (&'a str, &'a str, EdgeKind)>,
    {
        let priors = nodes
            .into_iter()
            .map(|(id, c)| (id.to_string(), c))
            .collect();
        let mut in_edges: HashMap<String, Vec<(String, EdgeKind)>> = HashMap::new();
        for (source, target, kind) in edges {
            in_edges
                .entry(target.to_string())
                .or_default()
                .push((source.to_string(), kind));
        }
        BeliefGraph {
            priors,
            in_edges,
            as_of: None,
            #[cfg(feature = "epistemic-redaction")]
            node_visibility: HashMap::new(),
            temporal: HashMap::new(),
            bitemporal_pin: None,
            #[cfg(feature = "evidence-graph")]
            evidence_loci: HashMap::new(),
        }
    }

    /// Test/utility: attach an explicit [`BiTemporalRecord`] to `id` — the fixture-side
    /// mirror of [`Self::from_graph_view`]'s blob decode, for exercising
    /// [`Self::at_instant`] without a `GraphView`.
    pub fn with_temporal(mut self, id: &str, record: BiTemporalRecord) -> Self {
        self.temporal.insert(id.to_string(), record);
        self
    }

    /// Test/utility: attach a complete governed locus to `id`.
    #[cfg(feature = "evidence-graph")]
    pub fn with_evidence_locus(mut self, id: &str, locus: eg_modality::EvidenceLocus) -> Self {
        self.evidence_loci.insert(id.to_string(), locus);
        self
    }

    /// Bitemporal narrowing (EPI-P3-5, CONCEPT:EG-KG.epistemic.bitemporal-reasoning):
    /// return a new projection containing only the nodes/edges live at `valid_ts`
    /// (valid time) AND `tx_ts` (transaction time) — either may be `None` to leave that
    /// axis unfiltered, so this also covers the single-axis case. A node with NO
    /// [`BiTemporalRecord`] passes through unfiltered on both axes (mirrors
    /// `eg-plan::exec::live_at`'s "absent metadata ⇒ always live" convention) — this
    /// narrows the GRAPH ITSELF (not just a downstream RowSet), so `why`/`why_not`/
    /// `propagate_confidence`/the TMS extensions all reason over the graph exactly as
    /// it stood at that instant, evidence that did not yet exist (or was already
    /// superseded) genuinely absent rather than merely tagged.
    pub fn at_instant(&self, valid_ts: Option<u64>, tx_ts: Option<u64>) -> BeliefGraph {
        let live = |id: &str| -> bool {
            self.temporal
                .get(id)
                .is_none_or(|rec| rec.live_at(valid_ts, tx_ts))
        };

        let priors: HashMap<String, f64> = self
            .priors
            .iter()
            .filter(|(id, _)| live(id))
            .map(|(id, c)| (id.clone(), *c))
            .collect();

        let mut in_edges: HashMap<String, Vec<(String, EdgeKind)>> = HashMap::new();
        for (target, edges) in &self.in_edges {
            if !live(target) {
                continue;
            }
            let kept: Vec<(String, EdgeKind)> =
                edges.iter().filter(|(src, _)| live(src)).cloned().collect();
            if !kept.is_empty() {
                in_edges.insert(target.clone(), kept);
            }
        }

        let temporal: HashMap<String, BiTemporalRecord> = self
            .temporal
            .iter()
            .filter(|(id, _)| live(id))
            .map(|(id, r)| (id.clone(), *r))
            .collect();

        BeliefGraph {
            priors,
            in_edges,
            as_of: self.as_of,
            #[cfg(feature = "epistemic-redaction")]
            node_visibility: self.node_visibility.clone(),
            temporal,
            bitemporal_pin: Some((valid_ts, tx_ts)),
            // Evidence loci are not bitemporal-scoped data (unlike priors/temporal),
            // so `at_instant` carries them through unfiltered — mirrors
            // `node_visibility`'s own unfiltered clone above.
            #[cfg(feature = "evidence-graph")]
            evidence_loci: self.evidence_loci.clone(),
        }
    }

    /// Attach explicit per-node [`RowVisibility`](eg_core::isolation::RowVisibility)
    /// (EPI-P3-4 test/utility constructor — a real caller builds this from a
    /// `GraphView` via [`Self::from_graph_view`]). Any id not given here defaults to
    /// untagged/hidden, exactly like a node with no property blob at all.
    #[cfg(feature = "epistemic-redaction")]
    pub fn with_visibility<'a, V>(mut self, visibility: V) -> Self
    where
        V: IntoIterator<Item = (&'a str, eg_core::isolation::RowVisibility)>,
    {
        for (id, vis) in visibility {
            self.node_visibility.insert(id.to_string(), vis);
        }
        self
    }
}

#[cfg(test)]
mod bitemporal_tests {
    use super::*;
    use crate::model::EdgeKind;

    fn record(valid_from: u64, tx_from: u64) -> BiTemporalRecord {
        BiTemporalRecord {
            valid_from: Some(valid_from),
            valid_until: None,
            tx_from: Some(tx_from),
            tx_to: None,
        }
    }

    // A node with no BiTemporalRecord at all is treated as always-live on both axes —
    // mirrors `eg-plan::exec::live_at`'s "absent metadata ⇒ from.unwrap_or(0)" fallback.
    #[test]
    fn node_with_no_temporal_record_is_always_live() {
        let bg = BeliefGraph::from_parts([("solo", 0.9)], Vec::<(&str, &str, EdgeKind)>::new());
        let narrowed = bg.at_instant(Some(0), Some(0));
        assert!(narrowed.priors.contains_key("solo"));
        let narrowed = bg.at_instant(Some(1_000_000), Some(1_000_000));
        assert!(narrowed.priors.contains_key("solo"));
    }

    // `at_instant` narrows on the VALID axis: a node not yet valid at `valid_ts` is
    // dropped even though it exists (transaction time unfiltered).
    #[test]
    fn at_instant_narrows_on_valid_axis() {
        let bg = BeliefGraph::from_parts([("claim", 0.9)], Vec::<(&str, &str, EdgeKind)>::new())
            .with_temporal("claim", record(100, 0));
        assert!(!bg.at_instant(Some(50), None).priors.contains_key("claim"));
        assert!(bg.at_instant(Some(150), None).priors.contains_key("claim"));
    }

    // `at_instant` narrows on the TRANSACTION axis independently of the valid axis.
    #[test]
    fn at_instant_narrows_on_transaction_axis() {
        let bg = BeliefGraph::from_parts([("claim", 0.9)], Vec::<(&str, &str, EdgeKind)>::new())
            .with_temporal("claim", record(0, 100));
        assert!(!bg.at_instant(None, Some(50)).priors.contains_key("claim"));
        assert!(bg.at_instant(None, Some(150)).priors.contains_key("claim"));
    }

    // BOTH axes at once: a claim recorded (tx) before it became valid can be live on
    // neither, one, or both axes depending on the query instant — the genuinely
    // bitemporal case (EPI-P3-5's acceptance criterion).
    #[test]
    fn at_instant_narrows_on_both_axes_simultaneously() {
        // Valid from t=100 (in the world); but the engine only RECORDED it at tx=200
        // (e.g. a late-arriving correction).
        let bg = BeliefGraph::from_parts([("claim", 0.9)], Vec::<(&str, &str, EdgeKind)>::new())
            .with_temporal(
                "claim",
                BiTemporalRecord {
                    valid_from: Some(100),
                    valid_until: None,
                    tx_from: Some(200),
                    tx_to: None,
                },
            );

        // Valid at the time, but not yet known to the engine at tx=150.
        let as_of_150 = bg.at_instant(Some(150), Some(150));
        assert!(!as_of_150.priors.contains_key("claim"));

        // Known to the engine (tx=250) AND valid as of that same instant.
        let as_of_250 = bg.at_instant(Some(150), Some(250));
        assert!(as_of_250.priors.contains_key("claim"));

        // Known to the engine, but querying validity at a time before it became true.
        let as_of_50_valid = bg.at_instant(Some(50), Some(250));
        assert!(!as_of_50_valid.priors.contains_key("claim"));
    }

    // Removing a node also drops any edge naming it as source OR target, and the
    // resulting graph is tagged with the pin for downstream `BeliefState.as_of`-style
    // provenance (via `bitemporal_pin`).
    #[test]
    fn at_instant_drops_edges_touching_a_filtered_node_and_tags_the_pin() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.5), ("late_evidence", 0.9)],
            [("late_evidence", "claim", EdgeKind::Supports)],
        )
        .with_temporal("late_evidence", record(500, 0));

        let narrowed = bg.at_instant(Some(100), None);
        assert!(!narrowed.priors.contains_key("late_evidence"));
        assert!(!narrowed.in_edges.contains_key("claim"));
        assert_eq!(narrowed.bitemporal_pin, Some((Some(100), None)));
    }
}
