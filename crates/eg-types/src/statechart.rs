//! `StatechartOp` — the wire op `Method::Statechart` carries (CONCEPT:INT-P2-2), gated
//! `statechart`. Mirrors `jobs::JobOp`'s "one `Method` variant, one internal op enum"
//! shape so the native statechart engine's whole control surface
//! (define/instantiate/send_event/get_state/list) costs the wire protocol exactly ONE
//! new `Method` variant.
//!
//! Pure serde — no dependency on `eg-statechart` (which sits ABOVE this crate in the
//! DAG). The rich definition MODEL (states/transitions/guards/actions) lives in
//! `eg-statechart`; rather than duplicate that whole typed tree here, [`StatechartOp::Define`]
//! carries it as an opaque, versioned MessagePack blob that the server decodes and
//! rebinds at the verified boundary — the SAME pattern `jobs::JobKind::ProgramOptimize`
//! uses for its nested `eg_program::OptimizationRequest`, keeping `eg-types` the bottom
//! of the crate DAG. The simpler ops carry only scalars + JSON context/payload.

use serde::{Deserialize, Serialize};

/// The statechart engine control-plane operation (CONCEPT:INT-P2-2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StatechartOp {
    /// Register a statechart DEFINITION. `def_msgpack` is a MessagePack-encoded
    /// `eg_statechart::StatechartDef` (opaque here — decoded + validated at the
    /// verified boundary). Content-addressed and idempotent server-side: the response
    /// carries the deterministic `def_id`.
    Define {
        #[serde(with = "serde_bytes")]
        def_msgpack: Vec<u8>,
    },
    /// Create a running INSTANCE of a stored definition in its initial state.
    /// `context` is the initial extended state (a JSON object, or null for empty).
    Instantiate {
        def_id: String,
        #[serde(default)]
        context: serde_json::Value,
    },
    /// Deliver an EVENT to a durable instance. `payload` is optional structured data
    /// guards/actions may read. `expected_version`, when present, is an OCC token —
    /// the send is rejected if the stored instance version has moved on.
    SendEvent {
        instance_id: String,
        event: String,
        #[serde(default)]
        payload: serde_json::Value,
        #[serde(default)]
        expected_version: Option<u64>,
    },
    /// Fetch (rehydrate) an instance's current durable `(state, context)` + version.
    GetState { instance_id: String },
    /// List instances owned by the caller, optionally filtered to one definition.
    List {
        #[serde(default)]
        def_id: Option<String>,
    },
}
