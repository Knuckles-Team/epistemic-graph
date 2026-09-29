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

mod carrier;
#[cfg(test)]
pub(crate) use carrier::tests as carrier_fixtures;
mod families;
pub use carrier::{CarrierRefusal, CARRIER_REFUSAL_CODE, ENGINE_INTERNAL_CODE};
pub use families::MethodWriteFamily;
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
mod method_11;
mod method_finish;
mod writeback;

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
pub(crate) use method_11::__eg_method_chunk_11;
pub(crate) use method_finish::__eg_method_finish;

__eg_method_chunk_0!();
impl Method {
    /// Every variant name compiled into this build, in declaration order. The
    /// method-policy registry is checked against this list instead of a
    /// hand-kept variant count (feature-gated variants follow the build).
    #[cfg(feature = "metrics")]
    pub fn variant_names() -> &'static [&'static str] {
        <Self as strum::VariantNames>::VARIANTS
    }

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

    /// [`Self::canonical_body_bytes`] of the `Method` one MessagePack request
    /// frame carries, decoded exactly as the transport decodes a frame
    /// (`rmp_serde::from_slice::<Request>`). A client that signs this value
    /// signs precisely what the server re-derives for the `eg2.` MAC, for every
    /// method, without restating field order, defaults, map ordering or
    /// float/byte widths. The frame's `auth_token` does not affect the result.
    pub fn canonical_body_of_request_frame(frame: &[u8]) -> Result<Vec<u8>, String> {
        let request: Request = rmp_serde::from_slice(frame).map_err(|error| {
            format!("request frame does not decode as a typed Request: {error}")
        })?;
        let body = request.method.canonical_body_bytes();
        if body.is_empty() {
            return Err("typed Method re-serialization failed".to_string());
        }
        Ok(body)
    }
}

#[cfg(test)]
mod canonical_frame_tests {
    use super::*;

    fn frame(params: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({
            "params": params,
            "method": "CancelRequest",
            "agent_id": null,
            "auth_token": "eg2.ignored",
            "graph": "g",
            "id": 1,
        }))
        .expect("a JSON frame encodes")
    }

    #[test]
    fn a_frame_yields_the_body_its_typed_method_re_serializes() {
        let expected = Method::CancelRequest { target_req_id: 7 }.canonical_body_bytes();
        let body = Method::canonical_body_of_request_frame(&frame(
            serde_json::json!({"target_req_id": 7}),
        ));
        assert_eq!(body, Ok(expected));
    }

    #[test]
    fn a_frame_the_transport_cannot_decode_is_refused() {
        let refused = Method::canonical_body_of_request_frame(&frame(
            serde_json::json!({"target_req_id": "seven"}),
        ));
        assert!(
            refused.is_err(),
            "an undecodable frame has no canonical body"
        );
    }
}
