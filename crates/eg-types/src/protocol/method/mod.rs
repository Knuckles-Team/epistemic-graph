use super::*;

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
