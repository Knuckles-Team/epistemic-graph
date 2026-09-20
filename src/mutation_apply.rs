//! Canonical durable mutation classification and application — facade-specific
//! remainder.
//!
//! Socket, embedded, and Raft execution share this deterministic implementation, so
//! a committed Method produces the same graph state on every path.
//!
//! **Hoisted (`plans/pyengine/EG-PYENGINE-PLAN.md` §4.2, `docs/architecture/
//! unified-inprocess-engine.md` §11 item 1):** the base graph-mutation set and the
//! `broker` family now live in `eg_core::durable_apply` — see that module's doc
//! comment for the full rationale. `crates/eg-pyengine` depends on `eg-core`
//! directly (below this facade in the workspace DAG, to avoid a cycle) and calls
//! that module, not this one.
//!
//! **What stayed here, and why:** four `Method` families genuinely cannot move
//! below the facade — `BatchUpdate`'s application calls `eg_compute::algorithms::
//! batch_update` (`eg-compute` depends on `eg-core`, not the reverse); `AddTriples`/
//! `RemoveTriples`/`DropNamedGraph` call `eg_rdf::mapping`/`eg_rdf::update` (same
//! direction); the mining/graphlearn write-back replay arms call
//! `crate::server::handlers::{mining,graphlearn}::replay` (`src/server` IS this
//! facade). Their matching `is_durable_mutation` classifier blocks
//! (`modality-serving`/`rdf`/`mining`×4/`graphlearn`) stay here too, since this
//! crate's own Cargo features (not `eg-core`'s) are what root `Cargo.toml` forwards
//! those facade feature flags into.
//!
//! `is_durable_mutation` checks these DAG-forced families first, then delegates the
//! base+`broker` set to `eg_core::durable_apply::is_durable_mutation`. `apply`
//! handles these same DAG-forced families explicitly, then delegates everything
//! else (its `_` arm) to `eg_core::durable_apply::apply`.

use crate::{graph::GraphCore, protocol::Method};

/// Stable serving principal for the engine's own kernel-owned durable writes.
///
/// This identity is shared by the server, embedded, and Raft/redb-only builds;
/// keeping it in the server-independent durable-application facade prevents a
/// standalone embedded build from acquiring a dependency on crate::server.
pub(crate) const ENGINE_LEDGER_PRINCIPAL: &str =
    "principal:sha256:41290b0e412ac542f312d4312a7a299e771eec66e3ffbbf7edb6369576875fb2";

/// True for the methods whose effect must survive a crash in the authoritative store.
///
/// EH-323 (same root cause as EH-316/EH-319, `write_classification.rs`'s module
/// doc has the full mechanism): `eg-capabilities` forces `eg-types`'s `rdf`,
/// `mining` and `graphlearn` features on unconditionally whenever this root
/// crate's `server` feature links it, so `AddTriples`/`RemoveTriples`/
/// `DropNamedGraph` and every `Mine*`/`GraphLearn*` `Method` variant are
/// wire-unconditional in ANY `server` build, regardless of whether this root
/// crate's OWN same-named facade feature is enabled. Gating a CLASSIFICATION
/// arm below behind that facade feature was therefore wrong the same way it
/// was in `write_classification.rs`: a durable write could be silently
/// misclassified non-durable (acknowledged, then lost on crash — EG-P0-3, the
/// exact defect class this file's own `MineSequence`/`MineForecast` comment
/// already names) under a build where the facade feature happens to be off
/// even though the variant, and the `Method` value carrying it, both exist.
/// `modality-serving` is NOT in `eg-capabilities`'s forced list (it has its
/// own separate mirrored lockstep feature there instead), so
/// `Method::ServedModality` genuinely does not exist without this crate's own
/// `modality-serving` feature; that arm keeps its cfg gate.
///
/// `apply`, below, is different on purpose: its matching arms call INTO
/// `eg_rdf`/`crate::server::handlers::{mining,graphlearn}`, dependencies that
/// truly do not link without this crate's own facade feature, so those arms
/// correctly stay gated — a replay that cannot run without the dependency
/// falls through to `eg_core::durable_apply::apply`'s `_` catch-all rather
/// than pretending to replay it. Making the classifier accurate is still the
/// right fix: it means a caller cannot end up with a genuinely durable write
/// that this function denies logging, on ANY build that can construct the
/// method at all — the separate, pre-existing question of replaying it back
/// across a feature-set change is unaffected either way.
pub fn is_durable_mutation(m: &Method) -> bool {
    // Served modality mutations use the stronger state-backed MutationBatch path;
    // raw source bytes are replaced by the state-backed receipt before this
    // classifier is consulted. It still reports the durable effect for policy and
    // placement accounting. `modality-serving` genuinely does not exist without
    // this crate's own feature (see the function doc); this is the one arm here
    // that keeps its cfg gate.
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = m {
        return op.mutates();
    }
    // `AddTriples` writes nodes + edges, so it is durable: the dispatch shell
    // records the Method and `apply` below re-parses + re-applies it
    // deterministically on replay, exactly like `BatchUpdate`. Wire-unconditional
    // (see function doc): no cfg gate.
    if matches!(
        m,
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
    ) {
        return true;
    }
    // Association-rule mining write-back (CONCEPT:EG-KG.mining.frequent-itemset-mining):
    // durable only when `writeback` materializes `:AssociationRule` nodes. `apply`
    // below re-mines + re-writes deterministically (explicit transactions reproduce
    // byte-identically; a graph-derived source re-derives from the graph, like the
    // broker/memory replay ops). A pure query (writeback=false) is not logged.
    //
    // `MineSequence`/`MineForecast` follow the SAME contract (`:SequentialPattern`/
    // `:Forecast` nodes) and MUST mirror `access::requires_write`'s classification —
    // they were previously missing here, so an acknowledged write was silently
    // dropped on crash and their `mining::replay` arms were dead code (EG-P0-3).
    // Wire-unconditional (see function doc): no cfg gate.
    if matches!(
        m,
        Method::MineAssociate {
            writeback: true,
            ..
        } | Method::MineCluster {
            writeback: true,
            ..
        } | Method::MineAnomaly {
            writeback: true,
            ..
        } | Method::MineClassifyPredict {
            writeback: true,
            ..
        } | Method::MineReduce {
            writeback: true,
            ..
        } | Method::MineSequence {
            writeback: true,
            ..
        } | Method::MineForecast {
            writeback: true,
            ..
        }
    ) {
        return true;
    }
    // `MineText`: durable only for `lda`/`nmf` writeback (their `:Topic` nodes) —
    // `tfidf` never mutates regardless of the flag, matching
    // `access::requires_write`'s exact condition byte-for-byte (EG-P0-3).
    // Wire-unconditional (see function doc): no cfg gate.
    if let Method::MineText {
        writeback,
        algorithm,
        ..
    } = m
    {
        return *writeback && !matches!(algorithm, crate::protocol::TextAlgorithm::Tfidf);
    }
    // `MineSubgraph`: durable only for `gspan` writeback (its `:FrequentSubgraph`
    // nodes) — `motif` never mutates regardless of the flag, matching
    // `access::requires_write`'s exact condition byte-for-byte (EG-P0-3).
    // Wire-unconditional (see function doc): no cfg gate.
    if let Method::MineSubgraph {
        writeback,
        algorithm,
        ..
    } = m
    {
        return *writeback && !matches!(algorithm, crate::protocol::SubgraphAlgorithm::Motif);
    }
    // Residual insight/mining families (CONCEPT:EG-KG.mining.entity-resolution /
    // causal-impact / process-mining / root-cause / risk-propagation /
    // ontology-gap / retrieval-quality / community-writeback): durable only when
    // `writeback` materializes their typed nodes; `apply` re-mines + re-writes
    // deterministically, same contract as the mining family above.
    // Wire-unconditional (see function doc): no cfg gate.
    if matches!(
        m,
        Method::MineEntityResolve {
            writeback: true,
            ..
        } | Method::MineCausalImpact {
            writeback: true,
            ..
        } | Method::MineProcess {
            writeback: true,
            ..
        } | Method::MineRootCause {
            writeback: true,
            ..
        } | Method::MineRiskPropagation {
            writeback: true,
            ..
        } | Method::MineOntologyGap {
            writeback: true,
            ..
        } | Method::MineRetrievalQuality {
            writeback: true,
            ..
        } | Method::MineCommunity {
            writeback: true,
            ..
        }
    ) {
        return true;
    }
    // Graph-learning write-back (CONCEPT:EG-KG.graphlearn.link-predictor): durable only
    // when `writeback` materializes `:EdgeFunction` / `:PredictedEdge` nodes. `apply`
    // re-derives + re-writes deterministically (seeded). A pure fit/predict is not logged.
    // Wire-unconditional (see function doc): no cfg gate.
    if matches!(
        m,
        Method::GraphLearnFit {
            writeback: true,
            ..
        } | Method::GraphLearnPredict {
            writeback: true,
            ..
        }
    ) {
        return true;
    }
    eg_core::durable_apply::is_durable_mutation(m)
}

/// Apply one durable mutation to a graph core. Mirrors the dispatch mutation
/// handlers for exactly the `is_durable_mutation` set.
///
/// This is the single canonical "durable Method → GraphCore mutation" path: durable log
/// replay (below) and the Raft state machine (CONCEPT:AU-KG.ingest.source-sync-canonical, `src/raft`) both
/// call it, so a committed Raft log entry applies BYTE-IDENTICALLY to how a
/// replayed durable mutation does. Deterministic (replaying the same Method over the
/// same pre-image yields the same state), which is the Raft state-machine contract.
///
/// The base+`broker` set is handled by `eg_core::durable_apply::apply` (this
/// function's `_` arm) — see this module's doc comment for why the arms below
/// (needing `eg-compute`/`eg-rdf`/`src/server`) could not move with it.
pub fn apply(core: &GraphCore, m: &Method) {
    match m {
        Method::BatchUpdate { operations_msgpack } => {
            let _ = crate::algorithms::batch_update(core, operations_msgpack);
        }
        // `AddTriples` (feature `rdf`): deterministic replay = re-parse the SAME
        // source text and re-apply the complete property-graph projection. The
        // multi-valued literal extras are embedded in the node blob, so this one
        // replay reconstructs the lossless dataset without a second authority.
        #[cfg(feature = "rdf")]
        Method::AddTriples { turtle, ntriples } => {
            if let Ok(triples) = parse_replayed_triples(turtle, ntriples) {
                let mut iris = eg_rdf::mapping::IriStore::default();
                // Rebuild the canonical property-graph RDF projection.
                let _ = eg_rdf::mapping::load_triples(core, &mut iris, "", triples);
            }
        }
        // `RemoveTriples` (feature `rdf`, CONCEPT:EG-KG.query.named-graph-support): deterministic replay =
        // re-parse the SAME source and re-retract. Idempotent — re-removing an absent
        // triple is a no-op. The lossless quad store is durable on its own file.
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { turtle, ntriples } => {
            if let Ok(triples) = parse_replayed_triples(turtle, ntriples) {
                let _ = eg_rdf::update::remove_triples(core, &triples);
            }
        }
        // `DropNamedGraph` (feature `rdf`, CONCEPT:EG-KG.query.named-graph-support): the named graph IS this
        // registry graph, so dropping it clears the whole core. The quad-store rows are
        // dropped durably on their own redb file at original execution, so replay only
        // needs to rebuild the empty in-graph state.
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => core.clear(),
        // Data-mining write-back (CONCEPT:EG-KG.mining.frequent-itemset-mining /
        // dbscan-density / isolation-forest): re-run the mining op + re-materialize
        // the `:AssociationRule` / `:Cluster` / `:Anomaly` nodes. The node ids are a
        // deterministic digest of the mined content, so replay is idempotent.
        #[cfg(all(feature = "mining", feature = "server"))]
        Method::MineAssociate { .. }
        | Method::MineCluster { .. }
        | Method::MineAnomaly { .. }
        | Method::MineClassifyPredict { .. }
        | Method::MineReduce { .. }
        | Method::MineSequence { .. }
        | Method::MineForecast { .. }
        | Method::MineText { .. }
        | Method::MineSubgraph { .. }
        | Method::MineEntityResolve { .. }
        | Method::MineCausalImpact { .. }
        | Method::MineProcess { .. }
        | Method::MineRootCause { .. }
        | Method::MineRiskPropagation { .. }
        | Method::MineOntologyGap { .. }
        | Method::MineRetrievalQuality { .. }
        | Method::MineCommunity { .. } => crate::server::handlers::mining::replay(core, m),
        // Graph-learning write-back (CONCEPT:EG-KG.graphlearn.link-predictor): re-run the
        // fit/predict + re-materialize the `:EdgeFunction` / `:PredictedEdge` nodes.
        // The node ids are a deterministic digest, so replay is idempotent.
        #[cfg(all(feature = "graphlearn", feature = "server"))]
        Method::GraphLearnFit { .. } | Method::GraphLearnPredict { .. } => {
            crate::server::handlers::graphlearn::replay(core, m)
        }
        _ => eg_core::durable_apply::apply(core, m),
    }
}

/// Re-parse a recorded `AddTriples`/`RemoveTriples` source for replay.
///
/// Replay is lenient where admission is strict: Turtle wins when both sources
/// are present, and an empty source replays as no triples. The committed Method
/// already passed admission, so replay must reproduce its effect, not re-judge it.
#[cfg(feature = "rdf")]
fn parse_replayed_triples(
    turtle: &str,
    ntriples: &str,
) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    if !turtle.trim().is_empty() {
        eg_rdf::mapping::parse_turtle(turtle)
    } else if !ntriples.trim().is_empty() {
        eg_rdf::mapping::parse_ntriples(ntriples)
    } else {
        Ok(Vec::new())
    }
}

/// EH-323 regression coverage: `DropNamedGraph` is a zero-field, wire-unconditional
/// `Method` variant (see `is_durable_mutation`'s module doc), so this test compiles
/// and runs under `--no-default-features --features server` with no `#[cfg]` gate
/// of its own needed for the variant to exist. Before the fix, `is_durable_mutation`
/// classified it non-durable under any build lacking this crate's own `rdf` facade
/// feature, even though the variant (and thus a request carrying it) exists —
/// exactly the "acknowledged, then silently lost on crash" defect class EG-P0-3
/// names elsewhere in this file. `AddTriples`/`RemoveTriples`/`MineText`/
/// `GraphLearnFit`/etc. got the identical mechanical fix (see the diff) but are not
/// independently re-tested here: several `Mine*` variants carry their OWN
/// wire-unconditional-but-facade-gated fields (`#[cfg(feature = "query")] plan`,
/// `#[cfg(feature = "epistemic")] as_claim`), which is the SAME defect class one
/// level down and is flagged, not chased, to keep this fix's diff reviewable.
#[cfg(test)]
mod eh323_slim_profile_tests {
    use super::is_durable_mutation;
    use crate::protocol::Method;

    #[test]
    fn drop_named_graph_is_durable_regardless_of_this_crates_own_rdf_feature() {
        assert!(
            is_durable_mutation(&Method::DropNamedGraph),
            "DropNamedGraph must classify durable regardless of this crate's own \
             `rdf` feature -- the variant is wire-unconditional"
        );
    }
}

#[cfg(all(test, feature = "rdf"))]
mod tests {
    use super::parse_replayed_triples;

    const TURTLE: &str = "<urn:a> <urn:p> <urn:b> .";
    const NTRIPLES: &str = "<urn:c> <urn:p> <urn:d> .\n<urn:e> <urn:p> <urn:f> .\n";

    #[test]
    fn replay_parse_prefers_turtle_then_ntriples_then_nothing() {
        assert_eq!(parse_replayed_triples(TURTLE, NTRIPLES).unwrap().len(), 1);
        assert_eq!(parse_replayed_triples("  ", NTRIPLES).unwrap().len(), 2);
        assert!(parse_replayed_triples(" ", "\n").unwrap().is_empty());
        assert!(parse_replayed_triples("not turtle <", NTRIPLES).is_err());
    }
}
