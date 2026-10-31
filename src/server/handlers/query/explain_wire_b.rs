use super::*;

#[cfg(feature = "epistemic-tms")]
pub(crate) fn resolve_conflict_wire(
    node_ids: &[String],
    semantics: &str,
    view: &crate::graph::GraphView,
) -> Result<crate::protocol::ResolveConflictResult, String> {
    let bg = medulla_epistemic::BeliefGraph::from_graph_view(view);
    let bg = restrict_belief_graph_to_component(&bg, node_ids);

    let (extension_sets, surviving, defeated, undecided): ConflictClassification = match semantics {
        "grounded" => resolve_conflict_grounded(&bg, node_ids),
        "preferred" | "stable" => {
            resolve_conflict_preferred_or_stable(&bg, node_ids, semantics == "preferred")
        }
        other => {
            return Err(format!(
                "ResolveConflict: unknown semantics '{other}' (expected \
                 grounded|preferred|stable)"
            ));
        }
    };

    // `extension_sets` reports each computed extension PROJECTED onto the caller's
    // requested `node_ids` — matching the scope `surviving`/`defeated`/`undecided`
    // already use. `BeliefGraph::from_graph_view` necessarily builds the argumentation
    // framework off every argument in the graph (Dung semantics need the whole attack
    // topology to compute an extension correctly), so an unfiltered extension would
    // leak every OTHER unattacked claim in the graph (which is trivially always "in"
    // any extension) into a response about a specific, caller-named conflict — turning
    // a targeted 3-argument query into a whole-graph dump. Filtering to `node_ids`
    // keeps the extension's SET MEMBERSHIP semantics for the queried arguments intact
    // (an id here really is in that extension) while not describing arguments the
    // caller never asked about.
    Ok(crate::protocol::ResolveConflictResult {
        semantics: semantics.to_string(),
        surviving,
        defeated,
        undecided,
        extension_sets: extension_sets
            .into_iter()
            .map(|e| e.into_iter().filter(|id| node_ids.contains(id)).collect())
            .collect(),
    })
}

/// The `"grounded"` arm of [`resolve_conflict_wire`]: each queried id is
/// `surviving` (in the grounded extension), `defeated` (attacked by some member of
/// it), or `undecided` (neither).
#[cfg(feature = "epistemic-tms")]
pub(crate) fn resolve_conflict_grounded(
    bg: &medulla_epistemic::BeliefGraph,
    node_ids: &[String],
) -> ConflictClassification {
    let grounded = medulla_epistemic::grounded_extension(bg);
    let mut surviving = Vec::new();
    let mut defeated = Vec::new();
    let mut undecided = Vec::new();
    for id in node_ids {
        if grounded.contains(id) {
            surviving.push(id.clone());
        } else if medulla_epistemic::augmented_attackers(bg, id)
            .iter()
            .any(|a| grounded.contains(a))
        {
            defeated.push(id.clone());
        } else {
            undecided.push(id.clone());
        }
    }
    (vec![grounded], surviving, defeated, undecided)
}

/// The `"preferred"`/`"stable"` arm of [`resolve_conflict_wire`]: each queried id
/// is `surviving` (in every extension), `defeated` (in no extension), or
/// `undecided` (in some but not all, or there are no extensions at all).
#[cfg(feature = "epistemic-tms")]
pub(crate) fn resolve_conflict_preferred_or_stable(
    bg: &medulla_epistemic::BeliefGraph,
    node_ids: &[String],
    preferred: bool,
) -> ConflictClassification {
    let extensions = if preferred {
        medulla_epistemic::preferred_extensions(bg)
    } else {
        medulla_epistemic::stable_extensions(bg)
    };
    let mut surviving = Vec::new();
    let mut defeated = Vec::new();
    let mut undecided = Vec::new();
    for id in node_ids {
        if extensions.is_empty() {
            undecided.push(id.clone());
        } else if extensions.iter().all(|e| e.contains(id)) {
            surviving.push(id.clone());
        } else if extensions.iter().all(|e| !e.contains(id)) {
            defeated.push(id.clone());
        } else {
            undecided.push(id.clone());
        }
    }
    (extensions, surviving, defeated, undecided)
}

/// Restrict `bg` to the weakly-connected component (over ALL its epistemic edges,
/// direction ignored) reachable from `seeds`. Dung argumentation semantics are
/// separable across disconnected components of the attack/support topology -- an
/// argument with no attack/support path to `seeds` cannot influence (or be
/// influenced by) any of them under grounded/preferred/stable semantics -- so this
/// changes nothing about `seeds`' own computed status. It DOES change the argument
/// COUNT `medulla_epistemic::tms` sees: unrestricted, `BeliefGraph::from_graph_view`
/// carries every node in the whole graph as a trivially-unattacked "argument"
/// (`from_graph_view` seeds `priors` from every node, whether or not it
/// participates in any epistemic edge), so a caller-scoped 3-argument query over a
/// graph that happens to also hold dozens of unrelated `Claim`s would otherwise
/// blow `tms::MAX_PREFERRED_ARGUMENTS` and silently fall back to a single
/// (grounded-only) extension -- misclassifying a genuinely credulous id as
/// `defeated` instead of `undecided`. Restricting to the component actually
/// relevant to `seeds` keeps the argument count proportional to the query, not the
/// whole graph.
#[cfg(feature = "epistemic-tms")]
pub(crate) fn restrict_belief_graph_to_component(
    bg: &medulla_epistemic::BeliefGraph,
    seeds: &[String],
) -> medulla_epistemic::BeliefGraph {
    let mut adjacency: std::collections::HashMap<&str, Vec<&str>> =
        std::collections::HashMap::new();
    for (target, edges) in &bg.in_edges {
        for (source, _kind) in edges {
            adjacency
                .entry(target.as_str())
                .or_default()
                .push(source.as_str());
            adjacency
                .entry(source.as_str())
                .or_default()
                .push(target.as_str());
        }
    }

    let mut reachable: std::collections::BTreeSet<String> = seeds.iter().cloned().collect();
    let mut stack: Vec<String> = seeds.to_vec();
    while let Some(node) = stack.pop() {
        if let Some(neighbors) = adjacency.get(node.as_str()) {
            for &neighbor in neighbors {
                if reachable.insert(neighbor.to_string()) {
                    stack.push(neighbor.to_string());
                }
            }
        }
    }

    let priors = bg
        .priors
        .iter()
        .filter(|(id, _)| reachable.contains(id.as_str()))
        .map(|(id, confidence)| (id.clone(), *confidence))
        .collect();
    let in_edges = bg
        .in_edges
        .iter()
        .filter(|(target, _)| reachable.contains(target.as_str()))
        .map(|(target, edges)| {
            (
                target.clone(),
                edges
                    .iter()
                    .filter(|(source, _)| reachable.contains(source.as_str()))
                    .cloned()
                    .collect(),
            )
        })
        .collect();
    medulla_epistemic::BeliefGraph {
        priors,
        in_edges,
        ..Default::default()
    }
}

/// `Method::WhatChanged` (EPI-P3-5, L53) — between two transaction times, which beliefs
/// changed and why, over the WHOLE graph (`medulla_epistemic::what_changed`).
#[cfg(feature = "epistemic-tms")]
pub(crate) fn what_changed_wire(
    view: &crate::graph::GraphView,
    tx_from: u64,
    tx_to: u64,
) -> crate::protocol::WhatChangedResult {
    let bg = medulla_epistemic::BeliefGraph::from_graph_view(view);
    let policy = medulla_epistemic::AuthorityPolicy::default();
    let changed = medulla_epistemic::what_changed(&bg, tx_from, tx_to, &policy);
    crate::protocol::WhatChangedResult {
        changed: changed
            .iter()
            .map(|c| crate::protocol::ChangedBeliefWire {
                id: c.id.clone(),
                believed_before: c.believed_before,
                believed_after: c.believed_after,
                confidence_before: c.confidence_before,
                confidence_after: c.confidence_after,
                evidence_added: c.evidence_added.clone(),
                evidence_removed: c.evidence_removed.clone(),
                reason: c.reason.clone(),
            })
            .collect(),
    }
}

/// SURPASS gap-closure ("unify the two evidence resolvers"): map an
/// `medulla_alignment::ResolvedArtifact` onto its wire twin.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
pub(crate) fn resolved_artifact_wire(
    r: medulla_alignment::ResolvedArtifact,
) -> crate::protocol::ResolvedArtifactWire {
    match r {
        medulla_alignment::ResolvedArtifact::Text {
            subject_ref,
            excerpt,
        } => crate::protocol::ResolvedArtifactWire {
            kind: "text".to_string(),
            subject_ref,
            excerpt: Some(excerpt),
            blob_ref: None,
            note: None,
            reason: None,
        },
        medulla_alignment::ResolvedArtifact::Blob {
            subject_ref,
            blob_ref,
            note,
        } => crate::protocol::ResolvedArtifactWire {
            kind: "blob".to_string(),
            subject_ref,
            excerpt: None,
            blob_ref: Some(blob_ref),
            note: Some(note),
            reason: None,
        },
        medulla_alignment::ResolvedArtifact::Unresolved {
            subject_ref,
            reason,
        } => crate::protocol::ResolvedArtifactWire {
            kind: "unresolved".to_string(),
            subject_ref,
            excerpt: None,
            blob_ref: None,
            note: None,
            reason: Some(unresolved_reason_wire(reason).to_string()),
        },
    }
}

/// Stable, machine-readable string form of `medulla_alignment::UnresolvedReason` for
/// `ResolvedArtifactWire.reason` — the resolver reason-code catalog GOC-05's
/// acceptance gates require ("Attach exact resolver outputs and every
/// unresolved reason code").
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
pub(crate) fn unresolved_reason_wire(reason: medulla_alignment::UnresolvedReason) -> &'static str {
    match reason {
        medulla_alignment::UnresolvedReason::MissingRendition => "missing_rendition",
        medulla_alignment::UnresolvedReason::CodecUnavailable => "codec_unavailable",
        medulla_alignment::UnresolvedReason::PolicyDenied => "policy_denied",
        medulla_alignment::UnresolvedReason::CorruptBytes => "corrupt_bytes",
        medulla_alignment::UnresolvedReason::OutOfRange => "out_of_range",
    }
}

/// X-1 (CONCEPT:MEDULLA-X1) — wire-project an `medulla_epistemic::EvidenceCitation`. `kind`
/// renders the `EdgeKind` via `Debug` (the SAME flat-string convention
/// `JustificationNodeWire::rule` uses for `JustRule`); `locus` reuses the
/// `evidence_locus_wire` mapper already defined above for `ExplainProvenance`.
/// SURPASS gap-closure ("unify the two evidence resolvers"): when `resolver` is
/// `Some` (the `alignment` feature is compiled in AND a blob store is configured —
/// see `explain_evidence_wire`), the citation's `locus` is ALSO resolved through it
/// (`medulla_alignment::EvidenceResolver::resolve`), attaching the REAL excerpt/blob
/// digest as `resolved` instead of leaving the caller with locus metadata alone.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
pub(crate) fn evidence_citation_wire(
    c: &medulla_epistemic::EvidenceCitation,
    resolver: Option<&crate::server::blob::cas_resolver::CasEvidenceResolver<'_>>,
) -> crate::protocol::EvidenceCitationWire {
    let resolved = resolver
        .and_then(|resolver| medulla_alignment::EvidenceResolver::resolve(resolver, &c.locus))
        .map(resolved_artifact_wire);
    crate::protocol::EvidenceCitationWire {
        evidence_id: c.evidence_id.clone(),
        kind: format!("{:?}", c.kind),
        locus: evidence_locus_wire(&c.locus),
        resolved,
    }
}

/// The `alignment`-less counterpart of the dual-gated `evidence_citation_wire`
/// above: `resolved` is always `None` (no `CasEvidenceResolver` exists to call in
/// this build) — same wire shape, honest absence rather than a fabricated
/// resolution.
#[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
pub(crate) fn evidence_citation_wire(
    c: &medulla_epistemic::EvidenceCitation,
) -> crate::protocol::EvidenceCitationWire {
    crate::protocol::EvidenceCitationWire {
        evidence_id: c.evidence_id.clone(),
        kind: format!("{:?}", c.kind),
        locus: evidence_locus_wire(&c.locus),
        resolved: None,
    }
}

/// `Method::ExplainEvidence` (CONCEPT:MEDULLA-X1) — build a `BeliefGraph` off `view` and
/// resolve `node_id`'s cited multimodal evidence (`medulla_epistemic::evidence_citations`).
/// SURPASS gap-closure ("unify the two evidence resolvers"): `blob_store`, when
/// `Some`, backs a `CasEvidenceResolver` over the SAME `view` snapshot so every
/// citation ALSO carries its resolved content — see `evidence_citation_wire`.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
pub(crate) fn explain_evidence_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
    blob_store: Option<std::sync::Arc<dyn crate::server::blob::ChunkStore>>,
) -> crate::protocol::ExplainEvidenceResult {
    let bg = medulla_epistemic::BeliefGraph::from_graph_view(view);
    let citations = medulla_epistemic::evidence_citations(&bg, node_id);

    // CONCEPT:MEDULLA-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b),
    // same idiom as the other epistemic handlers above. `ExplainEvidence` runs no belief
    // propagation (no posterior confidence is computed here), so `epistemic.status` is a
    // fixed descriptor rather than a believed/contested verdict; `contradiction_count`/
    // `policy_labels` come from a cheap local classification of `bg.in_edges` (already
    // loaded by `from_graph_view` above) by `EdgeKind` — not a new propagation pass.
    let ins = bg.in_edges.get(node_id).map(Vec::as_slice).unwrap_or(&[]);
    let supporting = ins
        .iter()
        .filter(|(_, k)| *k == medulla_epistemic::EdgeKind::Supports)
        .count();
    let contradicting = ins
        .iter()
        .filter(|(_, k)| {
            matches!(
                k,
                medulla_epistemic::EdgeKind::Contradicts | medulla_epistemic::EdgeKind::Attacks
            )
        })
        .count();
    let policy_labels =
        medulla_epistemic::classify_policy_labels(supporting, contradicting, 0).join(",");
    let _span = tracing::debug_span!(
        "epistemic.explain_evidence",
        epistemic.status = "cited",
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    let resolver = blob_store
        .map(|store| crate::server::blob::cas_resolver::CasEvidenceResolver::new(view, store));

    crate::protocol::ExplainEvidenceResult {
        citations: citations
            .iter()
            .map(|c| evidence_citation_wire(c, resolver.as_ref()))
            .collect(),
    }
}

/// The `alignment`-less counterpart of the dual-gated `explain_evidence_wire`
/// above: byte-for-byte the pre-existing behavior (locus metadata only, no
/// resolved content — there is no `CasEvidenceResolver` to build without the
/// `alignment` feature).
#[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
pub(crate) fn explain_evidence_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
) -> crate::protocol::ExplainEvidenceResult {
    let bg = medulla_epistemic::BeliefGraph::from_graph_view(view);
    let citations = medulla_epistemic::evidence_citations(&bg, node_id);

    let ins = bg.in_edges.get(node_id).map(Vec::as_slice).unwrap_or(&[]);
    let supporting = ins
        .iter()
        .filter(|(_, k)| *k == medulla_epistemic::EdgeKind::Supports)
        .count();
    let contradicting = ins
        .iter()
        .filter(|(_, k)| {
            matches!(
                k,
                medulla_epistemic::EdgeKind::Contradicts | medulla_epistemic::EdgeKind::Attacks
            )
        })
        .count();
    let policy_labels =
        medulla_epistemic::classify_policy_labels(supporting, contradicting, 0).join(",");
    let _span = tracing::debug_span!(
        "epistemic.explain_evidence",
        epistemic.status = "cited",
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    crate::protocol::ExplainEvidenceResult {
        citations: citations.iter().map(evidence_citation_wire).collect(),
    }
}

/// `Method::CausalEstimate` (EPI-P3-3/P3-6) — build an `medulla_epistemic::CausalGraph`
/// from the request-carried `variables` (rejecting an out-of-topological-order
/// parent as an explicit error, never a panic — mirrors
/// `CausalGraph::add_variable`'s own contract), then run whichever of the crate's
/// two non-counterfactual queries `mode` selects over `do_values`: the do-calculus
/// intervention (`CausalGraph::intervene`, `mode: Intervene` — the default) or the
/// observational conditioning query (`CausalGraph::observe`, `mode: Observe`).
/// Results are re-ordered to match the request's `variables` order (a `HashMap`
/// iteration order is not itself meaningful).
#[cfg(feature = "epistemic-causal")]
pub(crate) fn causal_estimate_wire(
    variables: &[crate::protocol::StructuralEquationWire],
    do_values: &std::collections::BTreeMap<String, f64>,
    mode: crate::protocol::CausalQueryModeWire,
) -> Result<crate::protocol::CausalEstimateResult, String> {
    let mut g = medulla_epistemic::CausalGraph::new();
    for v in variables {
        let parents: Vec<(&str, f64)> = v.parents.iter().map(|(p, w)| (p.as_str(), *w)).collect();
        g.add_variable(v.id.clone(), parents, v.bias, v.noise_var)?;
    }
    let values: std::collections::HashMap<String, f64> =
        do_values.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let estimates = match mode {
        crate::protocol::CausalQueryModeWire::Intervene => g.intervene(&values)?,
        crate::protocol::CausalQueryModeWire::Observe => g.observe(&values)?,
    };

    let ordered = variables
        .iter()
        .map(|v| {
            let est = estimates.get(&v.id).copied().unwrap_or_else(|| {
                unreachable!("intervene()/observe() return an estimate for every declared variable")
            });
            (
                v.id.clone(),
                crate::protocol::CausalEstimateWire {
                    mean: est.mean,
                    variance: est.variance,
                    interval: est.interval,
                    level: est.level,
                },
            )
        })
        .collect();
    Ok(crate::protocol::CausalEstimateResult { estimates: ordered })
}

/// `Method::CausalCounterfactual` (EPI-P3-6) — build an `medulla_epistemic::CausalGraph`
/// exactly as `causal_estimate_wire` does, then run Pearl's point-counterfactual
/// recipe (`CausalGraph::counterfactual`) over the request's fully-observed
/// `actual` unit and `do_values` intervention. Results (one point value per
/// variable) are re-ordered to match the request's `variables` order.
#[cfg(feature = "epistemic-causal")]
pub(crate) fn causal_counterfactual_wire(
    variables: &[crate::protocol::StructuralEquationWire],
    actual: &std::collections::BTreeMap<String, f64>,
    do_values: &std::collections::BTreeMap<String, f64>,
) -> Result<crate::protocol::CausalCounterfactualResult, String> {
    let mut g = medulla_epistemic::CausalGraph::new();
    for v in variables {
        let parents: Vec<(&str, f64)> = v.parents.iter().map(|(p, w)| (p.as_str(), *w)).collect();
        g.add_variable(v.id.clone(), parents, v.bias, v.noise_var)?;
    }
    let actual: std::collections::HashMap<String, f64> =
        actual.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let do_: std::collections::HashMap<String, f64> =
        do_values.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let cf = g.counterfactual(&actual, &do_)?;

    let ordered = variables
        .iter()
        .map(|v| {
            let val = cf.get(&v.id).copied().unwrap_or_else(|| {
                unreachable!("counterfactual() returns a value for every declared variable")
            });
            (v.id.clone(), val)
        })
        .collect();
    Ok(crate::protocol::CausalCounterfactualResult { values: ordered })
}

/// `Method::RankByProvenance` (EPI-P3-3) — map the request-carried
/// `RetrievalCandidateWire`s onto `medulla_epistemic::RetrievalCandidate` and run
/// `medulla_epistemic::rank` under the request's `RankWeightsWire`.
#[cfg(feature = "epistemic-causal")]
pub(crate) fn rank_by_provenance_wire(
    candidates: &[crate::protocol::RetrievalCandidateWire],
    weights: crate::protocol::RankWeightsWire,
) -> crate::protocol::RankByProvenanceResult {
    let candidates: Vec<medulla_epistemic::RetrievalCandidate> = candidates
        .iter()
        .map(|c| medulla_epistemic::RetrievalCandidate {
            id: c.id.clone(),
            similarity: c.similarity,
            source_reliability: c.source_reliability,
            freshness: c.freshness,
            calibration: c.calibration.map(|cal| medulla_epistemic::Calibration {
                interval: cal.interval,
                level: cal.level,
                evidence_count: cal.evidence_count,
            }),
        })
        .collect();
    let weights = medulla_epistemic::RankWeights {
        similarity: weights.similarity,
        evidence_quality: weights.evidence_quality,
    };
    let ranked = medulla_epistemic::rank(&candidates, weights);
    crate::protocol::RankByProvenanceResult {
        ranked: ranked
            .into_iter()
            .map(|r| crate::protocol::RankedResultWire {
                id: r.id,
                score: r.score,
                similarity: r.similarity,
                evidence_quality: r.evidence_quality,
            })
            .collect(),
    }
}
