use crate::protocol::Method;

use super::{NativeMutationCommand, SealedNativeMethod};

/// One declarative inventory of every public method with a native Raft command.
///
/// Each entry generates both the stable public method-name catalog and the
/// classifier used to construct the sealed command. Feature attributes apply to
/// classifier patterns only: the name inventory deliberately describes the
/// complete protocol schema in every build.
macro_rules! native_method_catalog {
    ($consumer:ident) => {
        $consumer! {
            record EvictLRU => GraphState,
            record DecaySweep => GraphState,
            record TouchNodes => GraphState,
            record FromMsgpack => GraphState,
            record Reconcile => GraphState,
            record ApplyMutation => GraphState,
            record PruneByLifecycle => GraphState,
            unit ClearLedger => GraphState,
            record ApplyLedger => GraphState,
            record CompactNodesByType => GraphState,
            #[cfg(feature = "epistemic")]
            record RecomputeMaterialization => GraphState,
            record Sql => GraphState,
            write CypherQuery => GraphState,
            #[cfg(feature = "graphql")]
            record GraphQl => GraphState,
            #[cfg(feature = "reasoning")]
            record RunDatalogReasoning => GraphState,
            record IcvConfigure => GraphState,
            record GraphSchema => GraphState,

            record BeginTxn => Transaction,
            record TxnAddNode => Transaction,
            record TxnRemoveNode => Transaction,
            record TxnAddEdge => Transaction,
            record TxnRemoveEdge => Transaction,
            record TxnCas => Transaction,
            record TxnAddEmbedding => Transaction,
            record TxnBlobRef => Transaction,
            #[cfg(feature = "tsdb")]
            record TxnAddMeasurement => Transaction,
            #[cfg(feature = "owl")]
            record TxnAxiom => Transaction,
            #[cfg(feature = "sparql")]
            record TxnConstruct => Transaction,
            #[cfg(feature = "query")]
            record TxnPlanWriteback => Transaction,
            #[cfg(feature = "epistemic")]
            record TxnMaterializeBelief => Transaction,
            record Commit => Transaction,
            record Rollback => Transaction,

            record ClaimWorkItem => WorkItem,
            record SubmitWorkItem => WorkItem,
            record SubmitWorkItems => WorkItem,
            record MintWorkItemClaimCapability => WorkItem,
            record RenewWorkItemLease => WorkItem,
            record CommitWorkItemResult => WorkItem,
            record CancelWorkItem => WorkItem,
            record DeferWorkItem => WorkItem,
            record CasWorkItemMetadata => WorkItem,
            record IssueControlLease => WorkItem,
            record TransitionControlLease => WorkItem,
            record ReserveWorkItemResources => WorkItem,
            record ReleaseWorkItemResources => WorkItem,
            record ReclaimWorkItemResources => WorkItem,
            record UpdateResourceHost => WorkItem,
            record AcquireCapacity => WorkItem,
            record RenewCapacity => WorkItem,
            record ReleaseCapacity => WorkItem,
            record ReclaimExpiredCapacity => WorkItem,
            record UpdateCapacityCell => WorkItem,

            #[cfg(feature = "blob")]
            record BlobBegin => Blob,
            #[cfg(feature = "blob")]
            record BlobChunkPut => Blob,
            #[cfg(feature = "blob")]
            record BlobCommit => Blob,
            #[cfg(feature = "blob")]
            unit BlobGc => Blob,
            #[cfg(feature = "blob")]
            record BlobRef => Blob,
            #[cfg(feature = "blob")]
            record BlobUnref => Blob,

            #[cfg(feature = "kv")]
            record KvPut => KeyValue,
            #[cfg(feature = "kv")]
            record KvDelete => KeyValue,
            #[cfg(feature = "kv")]
            record KvCas => KeyValue,

            #[cfg(feature = "tsdb")]
            record TsAppend => TimeSeries,
            #[cfg(feature = "tsdb")]
            record TsEvict => TimeSeries,
            #[cfg(feature = "tsdb")]
            record TsDeleteSeries => TimeSeries,

            #[cfg(feature = "jobs")]
            record AnalyticsJob => AnalyticsJob,
            #[cfg(feature = "sqlite-file")]
            record ImportSqliteFile => SqliteCatalog,

            record CreateChannel => SessionControl,
            record JoinChannel => SessionControl,
            record LeaveChannel => SessionControl,
            record CloseChannel => SessionControl,
            record SendMessage => SessionControl,
            #[cfg(feature = "federation")]
            record RegisterForeignSource => SessionControl,
            #[cfg(feature = "wasm-udf")]
            record RegisterUdf => SessionControl,
            #[cfg(feature = "streaming")]
            record RegisterContinuousQuery => SessionControl,
            #[cfg(feature = "streaming")]
            record DropContinuousQuery => SessionControl,
            #[cfg(feature = "streaming")]
            record RegisterTrigger => SessionControl,
            #[cfg(feature = "streaming")]
            record DropTrigger => SessionControl,
            #[cfg(all(feature = "streaming", feature = "stream"))]
            record CepSubscribe => SessionControl,
            #[cfg(all(feature = "streaming", feature = "stream"))]
            record CepUnsubscribe => SessionControl,

            record RegisterIdentity => Identity,
            record RbacAdmin => Identity,

            record Reshard => ClusterAdmin,
            record CatalogAssign => ClusterAdmin,
            record CatalogReassign => ClusterAdmin,
            record CatalogRemove => ClusterAdmin,
            record RebalanceExecute => ClusterAdmin,
            record Restore => ClusterAdmin,
            #[cfg(feature = "compute-dist")]
            record CreateMatView => ClusterAdmin,
            #[cfg(feature = "compute-dist")]
            record RefreshMatView => ClusterAdmin,
            #[cfg(feature = "matview")]
            record PlanMatViewDefine => ClusterAdmin,
            #[cfg(feature = "matview")]
            record PlanMatViewRefresh => ClusterAdmin,
            #[cfg(feature = "matview")]
            record PlanMatViewDrop => ClusterAdmin,

            record CreateGraph => GraphLifecycle,
            record DeleteGraph => GraphLifecycle,
            record ApplyMultisigMutation => Multisig,
            #[cfg(feature = "statechart")]
            record Statechart => Statechart,

            record ReserveDevelopmentLane => GraphState,
            record RenewDevelopmentLane => GraphState,
            record ObserveDevelopmentLane => GraphState,
            record FinishDevelopmentLane => GraphState,
            record CleanupDevelopmentLane => GraphState,
            record UpdateDevelopmentLaneQuota => GraphState,
        }
    };
}

macro_rules! declare_native_consensus_methods {
    ($( $(#[$attribute:meta])* $shape:ident $variant:ident => $domain:ident, )+) => {
        /// Public mutation tags with an explicit engine-native consensus command.
        ///
        /// The clustered-admission proof joins this complete protocol inventory
        /// with graph, fan-out, and self-routed commands.
        pub const NATIVE_CONSENSUS_METHODS: &[&str] = &[
            $(stringify!($variant),)+
        ];
    };
}

native_method_catalog!(declare_native_consensus_methods);

macro_rules! native_domains {
    ($consumer:ident) => {
        $consumer! {
            GraphState,
            Transaction,
            WorkItem,
            #[cfg(feature = "blob")]
            Blob,
            #[cfg(feature = "kv")]
            KeyValue,
            #[cfg(feature = "tsdb")]
            TimeSeries,
            #[cfg(feature = "jobs")]
            AnalyticsJob,
            #[cfg(feature = "statechart")]
            Statechart,
            #[cfg(feature = "sqlite-file")]
            SqliteCatalog,
            SessionControl,
            Identity,
            ClusterAdmin,
            GraphLifecycle,
            Multisig,
        }
    };
}

macro_rules! declare_native_domains {
    ($( $(#[$attribute:meta])* $domain:ident, )+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[repr(usize)]
        pub(crate) enum NativeMutationDomain {
            $(
                $(#[$attribute])*
                $domain,
            )+
        }

        pub(super) const NATIVE_DOMAIN_CONSTRUCTORS: &[
            fn(SealedNativeMethod) -> NativeMutationCommand
        ] = &[
            $(
                $(#[$attribute])*
                |sealed_method| NativeMutationCommand::$domain { sealed_method },
            )+
        ];
    };
}

native_domains!(declare_native_domains);

macro_rules! native_method_pattern {
    (record $variant:ident) => {
        Method::$variant { .. }
    };
    (unit $variant:ident) => {
        Method::$variant
    };
    (write $variant:ident) => {
        Method::$variant {
            mode: crate::protocol::CypherMode::Write,
            ..
        }
    };
}

macro_rules! declare_native_domain_classifier {
    ($( $(#[$attribute:meta])* $shape:ident $variant:ident => $domain:ident, )+) => {
        pub(super) fn native_domain(method: &Method) -> Option<NativeMutationDomain> {
            match method {
                $(
                    $(#[$attribute])*
                    native_method_pattern!($shape $variant) => Some(NativeMutationDomain::$domain),
                )+
                _ => None,
            }
        }
    };
}

native_method_catalog!(declare_native_domain_classifier);
