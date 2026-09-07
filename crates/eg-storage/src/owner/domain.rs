use crate::owner::layout::OwnerLayout;

mod sealed {
    pub trait Sealed {}
}

pub trait OwnerDomain: sealed::Sealed {
    const LAYOUT: OwnerLayout;
}

macro_rules! owner_domains {
    ($($owner:ident => $layout:ident),+ $(,)?) => {$(
        #[derive(Debug)]
        pub struct $owner;
        impl sealed::Sealed for $owner {}
        impl OwnerDomain for $owner {
            const LAYOUT: OwnerLayout = OwnerLayout::$layout;
        }
    )+};
}

owner_domains!(
    LedgerOnlyOwner => LedgerOnly,
    RbacOwner => Rbac,
    JobsOwner => Jobs,
    StatechartOwner => Statechart,
    TimeSeriesOwner => TimeSeries,
    KvOwner => Kv,
    BlobOwner => Blob,
    SemanticIndexOwner => SemanticIndex,
    SqlOwner => Sql,
    PathIndexOwner => PathIndex,
    RequestReplayOwner => RequestReplay,
    VizProvenanceOwner => VizProvenance,
    ColdTierOwner => ColdTier,
    TenantCatalogOwner => TenantCatalog,
    NodeInfoOwner => NodeInfo,
    ClusterHierarchyOwner => ClusterHierarchy,
    GraphShardOwner => GraphShard,
);
