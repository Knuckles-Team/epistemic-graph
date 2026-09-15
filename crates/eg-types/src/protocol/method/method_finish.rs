macro_rules! __eg_method_finish {
    (@acc [$($variants:tt)*]) => {
        /// All operations supported by the service.
        // `IntoStaticStr` (metrics builds) yields the variant name as the bounded
        // `op` label for request counters/histograms (CONCEPT:EG-KG.txn.per-graph-write-isolation).
        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[cfg_attr(feature = "metrics", derive(strum::IntoStaticStr))]
        #[serde(tag = "method", content = "params", deny_unknown_fields)]
        #[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
        pub enum Method {
            $($variants)*
        }
    };
}

pub(crate) use __eg_method_finish;
