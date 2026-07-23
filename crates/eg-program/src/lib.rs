//! Graph-native LM programs and governed self-improvement.
//!
//! `eg-program` provides a clean-room, DSPy-concept-compatible substrate whose
//! durable state is native to epistemic-graph. It deliberately contains no HTTP
//! client, provider SDK, LiteLLM bridge, secret resolution, disk cache, dynamic
//! code execution, or second mutation authority. Model-driven optimizers use the
//! injected [`ModelTransport`] contract; durable promotion must use the engine's
//! existing [`eg_types::ChangeEnvelope`] / `MutationBatch` authority.

mod commit;
mod contracts;
mod optimizer;
mod plan;
mod transport;

pub use commit::*;
pub use contracts::*;
pub use optimizer::*;
pub use plan::*;
pub use transport::*;
