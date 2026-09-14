use super::*;

#[cfg(any(feature = "mining", feature = "ml-pipeline"))]
use super::support_00::default_true;
#[cfg(feature = "mining")]
use super::support_00::{
    default_arima_d, default_arima_p, default_bucket_precision, default_confidence,
    default_damping, default_decay, default_eps, default_horizon, default_hw_alpha,
    default_hw_beta, default_hw_gamma, default_k, default_lda_alpha, default_lda_beta,
    default_lof_k, default_match_threshold, default_max_hops, default_max_iter,
    default_max_subgraph_edges, default_min_pts, default_n_trees, default_nu, default_resolution,
    default_risk_tolerance, default_sample_size, default_text_iterations, default_top_n,
    default_topic_k,
};
#[cfg(feature = "graphlearn")]
use super::support_01::default_gl_top_k;
#[cfg(feature = "mining")]
use super::support_01::{
    default_class_epochs, default_class_lr, default_knn_k, default_n_components, default_nb_alpha,
    default_reduce_epochs, default_svc_c, default_tsne_lr, default_tsne_perplexity,
    default_umap_min_dist, default_umap_neighbors,
};

mod method_00;
mod method_01;
mod method_02;
mod method_03;
mod method_04;
mod method_05;
mod method_06;
mod method_07;
mod method_08;
mod method_09;
mod method_10;
mod method_finish;

pub(crate) use method_00::__eg_method_chunk_0;
pub(crate) use method_01::__eg_method_chunk_1;
pub(crate) use method_02::__eg_method_chunk_2;
pub(crate) use method_03::__eg_method_chunk_3;
pub(crate) use method_04::__eg_method_chunk_4;
pub(crate) use method_05::__eg_method_chunk_5;
pub(crate) use method_06::__eg_method_chunk_6;
pub(crate) use method_07::__eg_method_chunk_7;
pub(crate) use method_08::__eg_method_chunk_8;
pub(crate) use method_09::__eg_method_chunk_9;
pub(crate) use method_10::__eg_method_chunk_10;
pub(crate) use method_finish::__eg_method_finish;

__eg_method_chunk_0!();
impl Method {
    /// The wire tag name for this variant (e.g. `"Ping"`, `"AddNode"`) — the
    /// adjacently-tagged `"method"` field serde already writes
    /// (`#[serde(tag = "method", content = "params")]`). Used by the v1
    /// signed-envelope path (CONCEPT:EG-KG.security.signed-request-envelope) to bind the
    /// operation name into the signature without needing the `metrics`
    /// feature's `strum::IntoStaticStr` derive, which isn't compiled into
    /// every serving tier.
    pub fn tag_name(&self) -> String {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v.get("method").and_then(|m| m.as_str().map(str::to_string)))
            .unwrap_or_default()
    }

    /// Deterministic byte encoding of this method (tag + params) for the v1
    /// envelope's body-hash binding (CONCEPT:EG-KG.security.signed-request-envelope). Reuses
    /// the SAME named-map MessagePack encoder the wire transport already uses
    /// (`rmp_serde::to_vec_named`) so the bytes are stable across the process
    /// (field order is the struct's declared order) without a bespoke
    /// canonicalizer. Hashing this (rather than transmitting a client-supplied
    /// hash) means the verifier NEVER trusts an attacker-stated body hash — it
    /// always recomputes it from the actual `method` that rode the wire.
    pub fn canonical_body_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).unwrap_or_default()
    }
}
