//! The build/completeness manifest every server-maintained index publishes
//! (CONCEPT:EG-KG.storage.index-manager-seam): its lifecycle state and the exact
//! source tuple it covers.

/// Lifecycle state of a maintained index build.  Merely registering an index
/// never makes it planner-visible; only `Valid` with a completeness cursor that
/// covers the source snapshot may be advertised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexValidity {
    Building,
    Valid,
    Stale,
    Failed,
}

/// Explicit source coverage for a maintained index.  The cursor is row-count
/// based because node/edge materialization is paged independently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexCompletenessCursor {
    pub nodes: u64,
    pub edges: u64,
    pub complete: bool,
}

/// Manifest published by every server-maintained index.  `build_version` is the
/// manifest schema/algorithm generation, not a filesystem or host identifier.
/// The source snapshot version and row counts are one tuple: a manifest is not
/// authoritative merely because it is marked `Valid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexManifest {
    pub source_snapshot_version: u64,
    pub build_version: u32,
    pub completeness: IndexCompletenessCursor,
    pub validity: IndexValidity,
}

impl IndexManifest {
    pub const BUILD_VERSION: u32 = 1;

    pub fn building(source_snapshot_version: u64, completeness: IndexCompletenessCursor) -> Self {
        Self {
            source_snapshot_version,
            build_version: Self::BUILD_VERSION,
            completeness,
            validity: IndexValidity::Building,
        }
    }

    pub fn valid(source_snapshot_version: u64, nodes: u64, edges: u64) -> Self {
        Self {
            source_snapshot_version,
            build_version: Self::BUILD_VERSION,
            completeness: IndexCompletenessCursor {
                nodes,
                edges,
                complete: true,
            },
            validity: IndexValidity::Valid,
        }
    }

    /// Whether the manifest is a safe base for an incremental delta.
    ///
    /// This is deliberately weaker than [`Self::covers_source`]: the write
    /// maintainer calls it while the caller still owns the graph transaction,
    /// before it can observe the post-mutation source row counts. Readiness and
    /// planner admission must always use `covers_source` instead.
    pub fn covers_version(&self, source_snapshot_version: u64) -> bool {
        self.build_version == Self::BUILD_VERSION
            && self.validity == IndexValidity::Valid
            && self.completeness.complete
            && self.source_snapshot_version == source_snapshot_version
    }

    /// Whether the manifest exactly describes the current source graph.
    ///
    /// Version-only checks were insufficient for recovered/catalog-only graphs:
    /// a stale node/edge cursor could still be `Valid` and advertise an index
    /// whose source coverage disagreed with the graph. Every readiness,
    /// reconciliation, and served-query decision must use this exact tuple so
    /// mismatches fail closed.
    pub fn covers_source(&self, source_snapshot_version: u64, nodes: u64, edges: u64) -> bool {
        self.build_version == Self::BUILD_VERSION
            && self.validity == IndexValidity::Valid
            && self.completeness.complete
            && self.source_snapshot_version == source_snapshot_version
            && self.completeness.nodes == nodes
            && self.completeness.edges == edges
    }
}

impl Default for IndexManifest {
    fn default() -> Self {
        Self::building(0, IndexCompletenessCursor::default())
    }
}
