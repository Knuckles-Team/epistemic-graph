//! Per-batch maintenance of the SERVER-LAYER secondary indexes (text / temporal /
//! derived-OWL) driven by [`super::IndexManager::commit_batch_at`]: whether a batch
//! delta may be applied over an index's current manifest, and what is published.

use super::{BatchMaintenance, ChangeSet, IndexManifest, IndexValidity, SecondaryIndex};
use crate::graph::GraphCore;

/// What maintaining one server-layer index did for a committed batch.
pub(super) enum ServerIndexStep {
    /// The batch delta was applied and `Valid` coverage published at the target version.
    Applied,
    /// A no-op batch over covered source: coverage carried forward, no delta applied.
    CarriedForward,
    /// The index could not safely take the delta (or failed it) and was marked stale.
    MarkedStale,
}

pub(super) fn record_server_index_step(tally: &mut BatchMaintenance, step: ServerIndexStep) {
    match step {
        ServerIndexStep::Applied => tally.deltas_applied += 1,
        ServerIndexStep::CarriedForward => {}
        ServerIndexStep::MarkedStale => tally.failures += 1,
    }
}

/// Maintain one server-layer index for a batch, publishing its manifest; `target` is
/// the `Valid` manifest for the post-batch source.
pub(super) fn maintain_server_index(
    idx: &dyn SecondaryIndex,
    core: &GraphCore,
    change: &ChangeSet,
    target: IndexManifest,
) -> ServerIndexStep {
    let prior = idx.manifest();
    // A delta is safe only over an index that completely covers the
    // pre-mutation source. Never let one later delta bless a stale or
    // partially materialized index as complete.
    let source_coverage_valid = delta_base_is_covered(&prior, core, change, &target);
    if idx.maintains_manifest() && !source_coverage_valid {
        publish_stale_manifest(idx, prior);
        return ServerIndexStep::MarkedStale;
    }
    if change.is_empty() {
        // Vector-only batches leave server-derived content unchanged but
        // still advance the graph version. Carry forward known-complete
        // coverage without rebuilding or applying a phantom delta.
        idx.publish_manifest(target);
        return ServerIndexStep::CarriedForward;
    }
    match idx.apply_delta(core, change) {
        Ok(()) => {
            idx.publish_manifest(target);
            ServerIndexStep::Applied
        }
        Err(_) => {
            publish_stale_manifest(idx, idx.manifest());
            ServerIndexStep::MarkedStale
        }
    }
}

/// Whether `prior` covers the source the batch delta applies to.
fn delta_base_is_covered(
    prior: &IndexManifest,
    core: &GraphCore,
    change: &ChangeSet,
    target: &IndexManifest,
) -> bool {
    if change.is_empty() {
        // With no topology delta, the supplied post-mutation counts
        // are also the pre-mutation source counts, so the exact
        // reconciliation predicate is available even under the held
        // topology guard.
        prior.covers_source(
            core.version(),
            target.completeness.nodes,
            target.completeness.edges,
        )
    } else {
        // Structural callers currently provide post-mutation counts;
        // retain the version gate for the pre-mutation delta base and
        // let the registry/read surfaces perform the exact tuple check.
        prior.covers_version(core.version())
    }
}

/// Publish `manifest` marked stale and incomplete (planner-ineligible).
fn publish_stale_manifest(idx: &dyn SecondaryIndex, mut manifest: IndexManifest) {
    manifest.validity = IndexValidity::Stale;
    manifest.completeness.complete = false;
    idx.publish_manifest(manifest);
}
