use super::*;

// ── EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────────────────

/// The RLS-filtered snapshot [`rls_snapshot`] builds, without that fn's `result-cache`
/// gating (the EXPLAIN surfaces are diagnostics-only and never participate in the
/// version-keyed result cache, so they need this unconditionally rather than
/// duplicating the inline result-cache-aware snapshot each `UnifiedQuery`-family arm
/// builds when `result-cache` is on).
#[cfg(feature = "query")]
pub(crate) fn explain_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> crate::graph::GraphView {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    snap
}

/// `EXPLAIN PLAN` — serialize `plan` as a `PlanDag` before/after the DAG-aware cost
/// optimizer (`eg_plan::optimize_dag`), annotated per-node with the SAME plan-time
/// cost/cardinality estimate (`eg_plan::ModalityCardinality`) the optimizer reordered on,
/// plus the active rule set (`eg_plan::cost_opt_rule_names()`). Pure diagnostics — no
/// execution, and no result rows are ever computed (GOC-12 acceptance gate 4: an
/// unbounded/uncosted EXPLAIN would tell an operator *that* a rewrite happened without
/// proving *why* it is cheaper).
///
/// `view`/`semantic` are the SAME RLS-filtered snapshot [`explain_snapshot`] built for the
/// caller, so every estimate here (candidate counts, selectivity, CPU/IO) is scoped to
/// what that agent may see — never the full unfiltered store — exactly mirroring the
/// authorize-before-rank invariant `run_unified`'s executor already enforces on actual
/// result rows (see `rank_op`'s candidate-allowlist doc in `eg-plan/src/exec.rs`).
#[cfg(feature = "query")]
pub(crate) fn explain_plan(
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::protocol::ExplainPlanResult, String> {
    use crate::protocol::{ExplainNodeWire, ExplainPlanResult};
    use eg_plan::{Cardinality, ModalityCardinality, PlanCtx, PlanStats};

    /// Annotate every node of `dag` with its plan-time cost/cardinality estimate, walked
    /// in topological order so a node's `estimated_rows_in` is always derived from its
    /// already-visited inputs' `estimated_rows_out` (never guessed out of order). A
    /// malformed dag (`topo_order` errors — dangling input / cycle) falls back to
    /// left-to-right node order with every node treated as its own source; this is a
    /// diagnostics-only surface, so a best-effort annotation beats refusing to EXPLAIN at
    /// all over a shape the optimizer itself already tolerates the same way elsewhere.
    fn to_wire(
        dag: &eg_plan::PlanDag,
        card: &ModalityCardinality,
        ctx: &PlanCtx,
    ) -> Vec<ExplainNodeWire> {
        let order = dag
            .topo_order()
            .unwrap_or_else(|_| (0..dag.nodes.len()).collect());
        let mut rows_out = vec![0.0f64; dag.nodes.len()];
        let mut wire: Vec<Option<ExplainNodeWire>> = vec![None; dag.nodes.len()];
        for id in order {
            let node = &dag.nodes[id];
            // A source has no inputs (`0.0` in); a single-input node inherits its one
            // predecessor's output; a multi-input fan-in/join sums its inputs' outputs —
            // a documented, safe over-estimate (never used to size an actual allocation).
            let in_card: f64 = node.inputs.iter().map(|&i| rows_out[i]).sum();
            let out_card = card.rows_out(&node.op, in_card, ctx);
            let selectivity = card.selectivity(&node.op, in_card, ctx);
            let cost = card.cost_of(&node.op, in_card, ctx);
            rows_out[id] = out_card;
            wire[id] = Some(ExplainNodeWire {
                id,
                op: format!("{:?}", node.op),
                inputs: node.inputs.clone(),
                estimated_rows_in: in_card,
                estimated_rows_out: out_card,
                estimated_selectivity: selectivity,
                estimated_cost_cpu: cost.cpu,
                estimated_cost_io: cost.io,
            });
        }
        wire.into_iter()
            .map(|w| w.expect("topo_order (or its fallback) visits every node exactly once"))
            .collect()
    }

    let ctx = PlanCtx::new(view, semantic);
    let card = ModalityCardinality::new(PlanStats::collect(&ctx));
    let before = eg_plan::PlanDag::from(plan);
    let after = eg_plan::optimize_dag(&before, &ctx);
    Ok(ExplainPlanResult {
        before: to_wire(&before, &card, &ctx),
        after: to_wire(&after, &card, &ctx),
        applied_rules: eg_plan::cost_opt_rule_names()
            .into_iter()
            .map(str::to_string)
            .collect(),
    })
}

/// Project the governed locus onto its DAG-safe wire mirror.
#[cfg(feature = "epistemic")]
pub(crate) fn evidence_locus_wire(
    locus: &eg_modality::EvidenceLocus,
) -> crate::protocol::EvidenceLocusWire {
    use crate::protocol::EvidenceLocusWire;
    EvidenceLocusWire {
        id: locus.id.as_ref().to_string(),
        subject: evidence_resource_wire(&locus.subject),
        address: evidence_address_wire(&locus.address),
        policy_ref: locus.policy_ref.to_string(),
        derivation_ref: locus.derivation_ref.as_ref().to_string(),
    }
}

#[cfg(feature = "epistemic")]
fn evidence_resource_wire(
    subject: &eg_modality::ResourceId,
) -> crate::protocol::EvidenceResourceWire {
    use crate::protocol::EvidenceResourceWire;
    use eg_modality::ResourceId;
    match subject {
        ResourceId::Artifact(id) => EvidenceResourceWire::Artifact(id.as_ref().to_string()),
        ResourceId::Occurrence(id) => EvidenceResourceWire::Occurrence(id.as_ref().to_string()),
        ResourceId::Rendition(id) => EvidenceResourceWire::Rendition(id.as_ref().to_string()),
        ResourceId::Segment(id) => EvidenceResourceWire::Segment(id.as_ref().to_string()),
        ResourceId::Feature(id) => EvidenceResourceWire::Feature(id.as_ref().to_string()),
        ResourceId::EvidenceLocus(id) => {
            EvidenceResourceWire::EvidenceLocus(id.as_ref().to_string())
        }
    }
}

#[cfg(feature = "epistemic")]
fn evidence_address_wire(
    address: &eg_modality::EvidenceAddress,
) -> crate::protocol::EvidenceAddressWire {
    use eg_modality::EvidenceAddress;
    if matches!(
        address,
        EvidenceAddress::CharacterRange { .. }
            | EvidenceAddress::TableCellRange { .. }
            | EvidenceAddress::ImageRegion { .. }
            | EvidenceAddress::PageRegion { .. }
            | EvidenceAddress::AudioRange { .. }
    ) {
        return evidence_address_wire_document(address);
    }
    evidence_address_wire_runtime(address)
}

#[cfg(feature = "epistemic")]
fn evidence_address_wire_document(
    address: &eg_modality::EvidenceAddress,
) -> crate::protocol::EvidenceAddressWire {
    use crate::protocol::EvidenceAddressWire;
    use eg_modality::EvidenceAddress;
    match address {
        EvidenceAddress::CharacterRange { start, end } => EvidenceAddressWire::CharacterRange {
            start: *start,
            end: *end,
        },
        EvidenceAddress::TableCellRange {
            row_start,
            row_end,
            col_start,
            col_end,
        } => EvidenceAddressWire::TableCellRange {
            row_start: *row_start,
            row_end: *row_end,
            col_start: *col_start,
            col_end: *col_end,
        },
        EvidenceAddress::ImageRegion {
            x,
            y,
            width,
            height,
        } => EvidenceAddressWire::ImageRegion {
            x: *x,
            y: *y,
            width: *width,
            height: *height,
        },
        EvidenceAddress::PageRegion {
            page,
            x,
            y,
            width,
            height,
        } => EvidenceAddressWire::PageRegion {
            page: *page,
            x: *x,
            y: *y,
            width: *width,
            height: *height,
        },
        EvidenceAddress::AudioRange { start_ms, end_ms } => EvidenceAddressWire::AudioRange {
            start_ms: *start_ms,
            end_ms: *end_ms,
        },
        _ => unreachable!("document evidence address was classified before conversion"),
    }
}

#[cfg(feature = "epistemic")]
fn evidence_address_wire_runtime(
    address: &eg_modality::EvidenceAddress,
) -> crate::protocol::EvidenceAddressWire {
    use crate::protocol::EvidenceAddressWire;
    use eg_modality::EvidenceAddress;
    match address {
        EvidenceAddress::VideoTimeRange { start_ms, end_ms } => {
            EvidenceAddressWire::VideoTimeRange {
                start_ms: *start_ms,
                end_ms: *end_ms,
            }
        }
        EvidenceAddress::FrameRange {
            start_frame,
            end_frame,
        } => EvidenceAddressWire::FrameRange {
            start_frame: *start_frame,
            end_frame: *end_frame,
        },
        EvidenceAddress::MetricWindow { start_ms, end_ms } => EvidenceAddressWire::MetricWindow {
            start_ms: *start_ms,
            end_ms: *end_ms,
        },
        EvidenceAddress::Point { x, y } => EvidenceAddressWire::Point { x: *x, y: *y },
        EvidenceAddress::RowVersion { row_ref, version } => EvidenceAddressWire::RowVersion {
            row_ref: row_ref.to_string(),
            version: *version,
        },
        EvidenceAddress::CodeSymbol {
            revision_ref,
            symbol_ref,
            start_line,
            end_line,
        } => EvidenceAddressWire::CodeSymbol {
            revision_ref: revision_ref.to_string(),
            symbol_ref: symbol_ref.to_string(),
            start_line: *start_line,
            end_line: *end_line,
        },
        EvidenceAddress::TraceSpan {
            trace_ref,
            span_ref,
        } => EvidenceAddressWire::TraceSpan {
            trace_ref: trace_ref.to_string(),
            span_ref: span_ref.to_string(),
        },
        _ => unreachable!("runtime evidence address was classified before conversion"),
    }
}

/// `EXPLAIN PROVENANCE` — run `plan` and, for each result row, resolve its EVIDENCE-FOR
/// provenance over the `KnowledgeSet` (E3) row shape (CONCEPT:EG-KG.query.knowledge-set),
/// reusing the SAME belief-substrate resolution `Op::EvidenceFor` runs, PLUS (X1,
/// CONCEPT:E4) the row's own located `evidence_refs` `KnowledgeSet::from_rowset`
/// already resolved. With `epistemic` off, every row's `source_refs`/`evidence_loci`
/// are empty and `resolved` is `false` — the documented "no epistemic resolution ran"
/// `KnowledgeSet` v1 default.
#[cfg(feature = "query")]
pub(crate) fn explain_provenance(
    request_id: u64,
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::epistemic_operations::EvidenceBundle, String> {
    use eg_plan::PlanCtx;

    let ctx = PlanCtx::new(view, semantic);
    let rs = eg_plan::execute(&plan, &ctx)?;
    let ks = eg_plan::KnowledgeSet::from_rowset(&rs, view, &[]);
    Ok(explain_provenance_result(request_id, &ks, &ctx))
}

/// `EXPLAIN PROVENANCE BY IDS` (CONCEPT:EG-KB-CURRENCY) — the ID-seeded sibling of
/// [`explain_provenance`]: builds the `KnowledgeSet` straight from an explicit id list
/// (`RowSet::from_ids`, deduplicated/first-occurrence, unranked — no `Op` plan/executor
/// involved) instead of running a `Plan`, then resolves the IDENTICAL per-row epistemic
/// columns via [`explain_provenance_result`]. The seam a caller with ids from ANY other
/// read path (Cypher, SQL, a prior `UnifiedQuery`) uses to fetch calibrated/cited/
/// time-versioned rows for exactly those ids.
#[cfg(feature = "query")]
pub(crate) fn explain_provenance_by_ids(
    request_id: u64,
    ids: Vec<String>,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::epistemic_operations::EvidenceBundle, String> {
    use eg_plan::PlanCtx;

    let ctx = PlanCtx::new(view, semantic);
    let rs = eg_plan::RowSet::from_ids(ids);
    let ks = eg_plan::KnowledgeSet::from_rowset(&rs, view, &[]);
    Ok(explain_provenance_result(request_id, &ks, &ctx))
}

/// Shared row-resolution core of [`explain_provenance`]/[`explain_provenance_by_ids`]
/// (CONCEPT:EG-KB-CURRENCY): map an already-built `KnowledgeSet`'s rows onto the wire
/// shape, widened beyond id/kind/source_refs/evidence_loci to also carry `score`/
/// `confidence`/`valid_time`/`tx_time`/`policy_labels` — straight field copies off each
/// `KnowledgeRow` (populated by `KnowledgeSet::from_rowset` regardless of `epistemic`
/// for score/confidence/valid_time/tx_time; `epistemic`-gated for
/// source_refs/policy_labels/evidence_loci exactly as before this widening).
#[cfg(feature = "query")]
pub(crate) fn explain_provenance_result(
    request_id: u64,
    ks: &eg_plan::KnowledgeSet,
    _ctx: &eg_plan::PlanCtx<'_>,
) -> crate::epistemic_operations::EvidenceBundle {
    use crate::epistemic_operations::{
        EvidenceBundle, EvidenceBundleSchemaVersion, EvidenceClaim, EvidenceTimeRange,
    };

    #[cfg(feature = "epistemic")]
    let claims: Vec<EvidenceClaim> = ks
        .rows
        .iter()
        .map(|row| {
            // Reuse the SAME `Op::EvidenceFor` resolution the plan-Op surface runs, as
            // its own tiny one-op plan — no private eg-plan access needed.
            let evidence_plan = eg_plan::Plan::new(vec![eg_plan::Op::EvidenceFor {
                claim_id: row.id.clone(),
            }]);
            let source_refs = eg_plan::execute(&evidence_plan, _ctx)
                .map(|r| r.ids())
                .unwrap_or_default();
            // X1: the row's own located evidence, already resolved by
            // `KnowledgeSet::from_rowset` — just map it onto the wire shape.
            let evidence_locus_refs = row
                .evidence_refs
                .iter()
                .map(|locus| locus.id.as_ref().to_string())
                .collect();
            EvidenceClaim {
                claim_ref: row.id.clone(),
                kind: row.kind.clone(),
                score: row.score.map(f64::from),
                confidence: row.confidence,
                valid_time: EvidenceTimeRange {
                    start_ms: row.valid_time.0,
                    end_ms: row.valid_time.1,
                },
                transaction_time: EvidenceTimeRange {
                    start_ms: row.tx_time.0,
                    end_ms: row.tx_time.1,
                },
                source_refs,
                evidence_locus_refs,
                contradiction_refs: row.contradiction_ids.clone(),
                proof_refs: row.proof_ids.clone(),
                policy_labels: row.policy_labels.clone(),
            }
        })
        .collect();
    #[cfg(not(feature = "epistemic"))]
    let claims: Vec<EvidenceClaim> = ks
        .rows
        .iter()
        .map(|row| EvidenceClaim {
            claim_ref: row.id.clone(),
            kind: row.kind.clone(),
            score: row.score.map(f64::from),
            confidence: row.confidence,
            valid_time: EvidenceTimeRange {
                start_ms: row.valid_time.0,
                end_ms: row.valid_time.1,
            },
            transaction_time: EvidenceTimeRange {
                start_ms: row.tx_time.0,
                end_ms: row.tx_time.1,
            },
            source_refs: Vec::new(),
            evidence_locus_refs: Vec::new(),
            contradiction_refs: Vec::new(),
            proof_refs: Vec::new(),
            policy_labels: Vec::new(),
        })
        .collect();

    EvidenceBundle {
        schema_version: EvidenceBundleSchemaVersion::V1,
        bundle_id: format!("request:{request_id}"),
        resolved: cfg!(feature = "epistemic"),
        answer_ref: None,
        claims,
        policy_exclusions: Vec::new(),
        next_action_refs: Vec::new(),
    }
}

/// `EXPLAIN POLICY` — run `plan` against BOTH the unfiltered snapshot and the caller's
/// RLS-filtered one (reusing the SAME `IsolationLayer::filter_view` every read path
/// already applies before this fn is ever reached), reporting which result ids the
/// policy denied. With no filtering applied (no `security` feature, or no caller/RLS on
/// this connection) `full_view` and `filtered_view` are the identical snapshot, so
/// `policy_denied_ids` is always empty.
#[cfg(feature = "query")]
pub(crate) fn explain_policy(
    plan: eg_plan::Plan,
    full_view: &crate::graph::GraphView,
    filtered_view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::protocol::ExplainPolicyResult, String> {
    use crate::protocol::ExplainPolicyResult;
    use eg_plan::PlanCtx;

    let full_ctx = PlanCtx::new(full_view, semantic);
    let filtered_ctx = PlanCtx::new(filtered_view, semantic);
    let full_ids: std::collections::HashSet<String> = eg_plan::execute(&plan, &full_ctx)?
        .ids()
        .into_iter()
        .collect();
    let visible_ids: Vec<String> = eg_plan::execute(&plan, &filtered_ctx)?.ids();
    let visible_set: std::collections::HashSet<&str> =
        visible_ids.iter().map(String::as_str).collect();
    let mut policy_denied_ids: Vec<String> = full_ids
        .iter()
        .filter(|id| !visible_set.contains(id.as_str()))
        .cloned()
        .collect();
    policy_denied_ids.sort();
    Ok(ExplainPolicyResult {
        visible_ids,
        policy_denied_ids,
    })
}

/// Count `DerivedSupport`/`DerivedContradiction` nodes in an already-computed proof
/// tree (CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span
/// attributes, WS-1b): `(supporting, contradicting)`. A cheap walk over data
/// `explain_belief_tree`/`epistemic_status` already built above — not a new epistemic
/// computation — feeding `eg_epistemic::classify_policy_labels` for the
/// `epistemic.policy_labels` span attribute.
#[cfg(feature = "epistemic")]
pub(crate) fn count_tree_rules(node: &eg_epistemic::ProofNode) -> (usize, usize) {
    let (mut supporting, mut contradicting) = match node.rule {
        eg_epistemic::JustRule::DerivedSupport => (1, 0),
        eg_epistemic::JustRule::DerivedContradiction => (0, 1),
        eg_epistemic::JustRule::Asserted | eg_epistemic::JustRule::BayesianUpdate => (0, 0),
    };
    for p in &node.premises {
        let (s, c) = count_tree_rules(p);
        supporting += s;
        contradicting += c;
    }
    (supporting, contradicting)
}

/// Wire-project one [`eg_epistemic::ProofNode`] (recursively) — shared by the classic
/// `explain_belief` below AND the L53 `epistemic_status_wire` capstone, so both surfaces
/// render a proof tree identically.
#[cfg(feature = "epistemic")]
pub(crate) fn proof_node_wire(
    node: &eg_epistemic::ProofNode,
) -> crate::protocol::JustificationNodeWire {
    crate::protocol::JustificationNodeWire {
        claim: node.claim.clone(),
        rule: format!("{:?}", node.rule),
        confidence: node.confidence,
        premises: node.premises.iter().map(proof_node_wire).collect(),
    }
}

/// `EXPLAIN BELIEF <node_id>` — the FULL, un-flattened E1 justification tree
/// (`eg_epistemic::JustificationGraph`, via `eg_plan::explain_belief_tree`), wire-projected
/// recursively (mirroring `Method::OwlExplain`'s `ProofNodeWire`).
#[cfg(feature = "epistemic")]
pub(crate) fn explain_belief(
    node_id: &str,
    view: &crate::graph::GraphView,
) -> crate::protocol::ExplainBeliefResult {
    // A minimal semantic store: `explain_belief_tree` only reads `ctx.view` +
    // `ctx.belief_policy` (default, unbound on this path — no facade caller binds a
    // tenant-specific policy today, matching every other served epistemic op).
    let semantic = eg_core::compute::semantic::SemanticStore::new();
    let ctx = eg_plan::PlanCtx::new(view, &semantic);
    let tree = eg_plan::explain_belief_tree(&ctx, node_id);

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b).
    // Mirrors the `write_coalescer.apply_batch`/`ann_index_build` span idiom (a plain
    // `tracing::debug_span!(...).entered()` guard over a sync block): additive-only,
    // exported by the SAME `tracing-opentelemetry` layer `otel.rs` installs when built
    // `--features otel` with `EPISTEMIC_GRAPH_OTLP_ENDPOINT` set, a complete no-op
    // otherwise. Every value is read off `tree`, already computed above — no new
    // epistemic computation.
    let (supporting, contradicting) = count_tree_rules(&tree.root);
    let policy_labels =
        eg_epistemic::classify_policy_labels(supporting, contradicting, 0).join(",");
    let _span = tracing::debug_span!(
        "epistemic.explain_belief",
        epistemic.confidence = tree.root.confidence,
        epistemic.status = tracing::field::debug(tree.root.rule),
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    crate::protocol::ExplainBeliefResult {
        root: proof_node_wire(&tree.root),
    }
}

/// Redacted-tree sibling of [`count_tree_rules`] — same walk, over
/// `eg_epistemic::RedactedProofNode` instead of `ProofNode` (a redaction preserves
/// `rule`/`confidence`/`premises` per that type's own doc comment, so the walk is
/// identical; the two types just aren't unified by a shared trait).
#[cfg(feature = "epistemic-redaction")]
pub(crate) fn count_redacted_tree_rules(node: &eg_epistemic::RedactedProofNode) -> (usize, usize) {
    let (mut supporting, mut contradicting) = match node.rule {
        eg_epistemic::JustRule::DerivedSupport => (1, 0),
        eg_epistemic::JustRule::DerivedContradiction => (0, 1),
        eg_epistemic::JustRule::Asserted | eg_epistemic::JustRule::BayesianUpdate => (0, 0),
    };
    for p in &node.premises {
        let (s, c) = count_redacted_tree_rules(p);
        supporting += s;
        contradicting += c;
    }
    (supporting, contradicting)
}

/// L51 — the redaction-aware sibling of [`explain_belief`]: builds a [`BeliefGraph`]
/// straight off `view` (populating `node_visibility` from the SAME per-node RLS blob
/// `filter_view` reads elsewhere, since `epistemic-redaction` turns that decode on in
/// `BeliefGraph::from_graph_view`) and routes through
/// `eg_epistemic::explain_belief_redacted_capped` under `actor_id`. `cap` is the
/// wire-requested `DisclosureLevelWire`, converted 1:1 to `eg_epistemic::DisclosureLevel`
/// (never a grant — see that fn's doc comment).
#[cfg(feature = "epistemic-redaction")]
pub(crate) fn explain_belief_redacted_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
    cap: crate::protocol::DisclosureLevelWire,
    isolation: &crate::isolation::IsolationLayer,
    actor_id: &str,
) -> crate::protocol::ExplainBeliefRedactedResult {
    use crate::protocol::{ExplainBeliefRedactedResult, RedactedJustificationNodeWire};

    fn redacted_node_wire(node: &eg_epistemic::RedactedProofNode) -> RedactedJustificationNodeWire {
        RedactedJustificationNodeWire {
            claim: node.claim.clone(),
            redaction_label: node.redaction_label.clone(),
            rule: format!("{:?}", node.rule),
            confidence: node.confidence,
            premises: node.premises.iter().map(redacted_node_wire).collect(),
        }
    }

    let cap = disclosure_level_from_wire(cap);

    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let redacted = eg_epistemic::explain_belief_redacted_capped(
        &bg,
        node_id,
        &policy,
        isolation,
        actor_id,
        Some(cap),
    );

    let level = disclosure_level_to_wire(redacted.level);
    let existence = existence_signal_to_wire(redacted.existence);

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b),
    // same idiom as `explain_belief` above. `redacted.existence` (Supported/Contradicted/
    // Uncertain) is exactly the "status" concept for a redaction-capped read — the
    // caller's RLS actor may not see enough of the tree to justify a finer label.
    // `root` is `None` at `ExistenceOnly` (no structure rendered at all, by design), so
    // confidence/contradiction_count/policy_labels are only recorded when a tree is
    // actually present — never fabricated.
    let (confidence, contradicting, policy_labels) = redacted_tree_summary(redacted.root.as_ref());
    let _span = tracing::debug_span!(
        "epistemic.explain_belief_redacted",
        epistemic.confidence = tracing::field::Empty,
        epistemic.status = tracing::field::debug(existence),
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();
    // `confidence` is `Option<f64>` (unavailable at `ExistenceOnly`, see above) —
    // `tracing::field::Value` has no blanket `Option` impl, so record it after span
    // creation only when present, leaving the field `Empty` otherwise (never a
    // fabricated `0.0`).
    if let Some(c) = confidence {
        tracing::Span::current().record("epistemic.confidence", c);
    }

    ExplainBeliefRedactedResult {
        level,
        existence,
        root: redacted.root.as_ref().map(redacted_node_wire),
    }
}

#[cfg(feature = "epistemic-redaction")]
pub(crate) fn disclosure_level_from_wire(
    cap: crate::protocol::DisclosureLevelWire,
) -> eg_epistemic::DisclosureLevel {
    match cap {
        crate::protocol::DisclosureLevelWire::Full => eg_epistemic::DisclosureLevel::Full,
        crate::protocol::DisclosureLevelWire::Skeleton => eg_epistemic::DisclosureLevel::Skeleton,
        crate::protocol::DisclosureLevelWire::ExistenceOnly => {
            eg_epistemic::DisclosureLevel::ExistenceOnly
        }
    }
}

#[cfg(feature = "epistemic-redaction")]
pub(crate) fn disclosure_level_to_wire(
    level: eg_epistemic::DisclosureLevel,
) -> crate::protocol::DisclosureLevelWire {
    match level {
        eg_epistemic::DisclosureLevel::Full => crate::protocol::DisclosureLevelWire::Full,
        eg_epistemic::DisclosureLevel::Skeleton => crate::protocol::DisclosureLevelWire::Skeleton,
        eg_epistemic::DisclosureLevel::ExistenceOnly => {
            crate::protocol::DisclosureLevelWire::ExistenceOnly
        }
    }
}

#[cfg(feature = "epistemic-redaction")]
pub(crate) fn existence_signal_to_wire(
    existence: eg_epistemic::ExistenceSignal,
) -> crate::protocol::ExistenceSignalWire {
    match existence {
        eg_epistemic::ExistenceSignal::Supported => crate::protocol::ExistenceSignalWire::Supported,
        eg_epistemic::ExistenceSignal::Contradicted => {
            crate::protocol::ExistenceSignalWire::Contradicted
        }
        eg_epistemic::ExistenceSignal::Uncertain => crate::protocol::ExistenceSignalWire::Uncertain,
    }
}

/// The confidence/contradiction-count/policy-labels summary of
/// [`explain_belief_redacted_wire`]'s OTEL span attributes: `None`/`0`/empty when
/// `root` is `None` (`ExistenceOnly` renders no structure at all, by design) —
/// never fabricated.
#[cfg(feature = "epistemic-redaction")]
pub(crate) fn redacted_tree_summary(
    root: Option<&eg_epistemic::RedactedProofNode>,
) -> (Option<f64>, usize, String) {
    match root {
        Some(root) => {
            let (supporting, contradicting) = count_redacted_tree_rules(root);
            (
                Some(root.confidence),
                contradicting,
                eg_epistemic::classify_policy_labels(supporting, contradicting, 0).join(","),
            )
        }
        None => (None, 0, String::new()),
    }
}

/// L53 — wire-project an `eg_epistemic::query::WhyNot` (see `WhyNotWire` docs).
#[cfg(feature = "epistemic-tms")]
pub(crate) fn why_not_wire(wn: &eg_epistemic::WhyNot) -> crate::protocol::WhyNotWire {
    use crate::protocol::WhyNotWire;
    let (reason, blockers, competing) = match &wn.reason {
        eg_epistemic::WhyNotReason::Unknown => ("Unknown", Vec::new(), Vec::new()),
        eg_epistemic::WhyNotReason::InsufficientConfidence => {
            ("InsufficientConfidence", Vec::new(), Vec::new())
        }
        eg_epistemic::WhyNotReason::Contradicted { blockers } => {
            ("Contradicted", blockers.clone(), Vec::new())
        }
        eg_epistemic::WhyNotReason::Undecided { competing } => {
            ("Undecided", Vec::new(), competing.clone())
        }
    };
    WhyNotWire {
        claim: wn.claim.clone(),
        reason: reason.to_string(),
        blockers,
        competing,
        confidence: wn.confidence,
    }
}

/// L53 — wire-project an `eg_epistemic::MinimalFlipSet` ("what would invalidate it").
#[cfg(feature = "epistemic-tms")]
pub(crate) fn minimal_flip_set_wire(
    f: &eg_epistemic::MinimalFlipSet,
) -> crate::protocol::MinimalFlipSetWire {
    crate::protocol::MinimalFlipSetWire {
        claim: f.claim.clone(),
        believed_now: f.believed_now,
        evidence_ids: f.evidence_ids.iter().cloned().collect(),
        believed_after: f.believed_after,
    }
}

/// `Method::EpistemicStatus` (EPI-P3-5, L53) — the Phase-3 acceptance capstone: build a
/// [`BeliefGraph`] off `view` and run `eg_epistemic::epistemic_status` under the
/// default `AuthorityPolicy` (no facade caller binds a tenant-specific policy today,
/// matching `explain_belief`'s own posture), wire-projecting every facet.
#[cfg(feature = "epistemic-tms")]
pub(crate) fn epistemic_status_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
) -> crate::protocol::EpistemicStatusResult {
    use crate::protocol::{AuthorityPolicyWire, EpistemicStatusResult, EpistemicStatusWire};

    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let status = eg_epistemic::epistemic_status(&bg, node_id, &policy);

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b),
    // same idiom as `explain_belief`/`explain_belief_redacted_wire` above. `EpistemicStatus`
    // is the richest already-computed source of the four (`believed`/`confidence`/
    // `contradicting`/`evidence`/`attacking` are all fields `eg_epistemic::epistemic_status`
    // already populated above) — no new epistemic computation, just reading them before
    // they're moved into the wire struct below.
    let policy_labels = eg_epistemic::classify_policy_labels(
        status.evidence.len(),
        status.contradicting.len(),
        status.attacking.len(),
    )
    .join(",");
    let _span = tracing::debug_span!(
        "epistemic.epistemic_status",
        epistemic.confidence = status.confidence,
        epistemic.status = if status.believed { "believed" } else { "not_believed" },
        epistemic.contradiction_count = status.contradicting.len(),
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    EpistemicStatusResult {
        status: EpistemicStatusWire {
            claim: status.claim,
            believed: status.believed,
            confidence: status.confidence,
            uncertainty: status.uncertainty,
            proof: proof_node_wire(&status.proof.root),
            why_not: status.why_not.as_ref().map(why_not_wire),
            evidence: status.evidence,
            contradicting: status.contradicting,
            attacking: status.attacking,
            authority: AuthorityPolicyWire {
                source_reliability: status.authority.source_reliability,
                attack_multiplier: status.authority.attack_multiplier,
                prior_strength: status.authority.prior_strength,
            },
            valid_time: status.valid_time,
            tx_time: status.tx_time,
            what_would_invalidate: status
                .what_would_invalidate
                .as_ref()
                .map(minimal_flip_set_wire),
        },
    }
}

/// `Method::ResolveConflict` (EPI-P3-7, gap-fill) — build a `BeliefGraph` off `view`
/// and run the Dung argumentation semantics `semantics` names
/// (`eg_epistemic::tms::{grounded_extension,preferred_extensions,stable_extensions}`
/// — reused as-is, never reimplemented), then partition `node_ids` into
/// surviving/defeated/undecided against the computed extension(s):
///
/// * `"grounded"`: the unique extension only ever gives the IN (accepted) set
///   directly (`grounded_extension`), so OUT vs UNDECIDED is recovered from the
///   SAME public API rather than needing the crate's private 3-way labelling: an id
///   NOT in the extension is `defeated` (OUT) iff at least one of its augmented
///   (bipolar-closed) attackers IS in the extension (`augmented_attackers`) —
///   otherwise it is `undecided` (caught in an unresolved/paraconsistent conflict,
///   e.g. an odd attack cycle, matching `eg_epistemic::tms`'s own module docs).
/// * `"preferred"`/`"stable"`: potentially several credulous extensions. An id in
///   EVERY extension `survives` (unanimous across every admissible "side"); an id in
///   NONE is `defeated` (never credulously acceptable); anything in SOME but not all
///   is `undecided` (contested). When `semantics` legitimately yields NO extension
///   at all (a real `stable` result over e.g. an odd cycle, or the crate's own
///   NP-hardness argument-count/search-budget caps firing — see `tms` module docs),
///   every requested id is reported `undecided` rather than fabricating a verdict.
///
/// An id in `node_ids` that names no argument anywhere in the graph degrades to
/// `undecided` (never fabricated as surviving/defeated) — the same "no signal ⇒ safe
/// default" convention every other epistemic op follows.
#[cfg(feature = "epistemic-tms")]
/// `(extension_sets, surviving, defeated, undecided)` — the shared classification
/// shape [`resolve_conflict_grounded`] and [`resolve_conflict_preferred_or_stable`]
/// both produce for [`resolve_conflict_wire`].
pub(crate) type ConflictClassification = (
    Vec<std::collections::BTreeSet<String>>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
);
