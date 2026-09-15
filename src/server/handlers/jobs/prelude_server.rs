//! Shared server and feature-gated imports for private analytics-job modules.

#[cfg(feature = "program-optimization")]
pub(super) use eg_modality::{Classification, OpaqueRef, PolicyEnvelope};
#[cfg(feature = "program-optimization")]
pub(super) use eg_program::{
    NativeCompiler, OptimizationRequest, ProgramModality, ProgramRevisionIdentity,
};

pub(super) use crate::isolation::AccessLevel;
pub(super) use crate::mutation_batch::{DurabilityDomain, MutationSurface};
pub(super) use crate::protocol::{Method, Response, ResultPayload};
pub(super) use crate::server::access::check_graph_access;
pub(super) use crate::server::state::ServerState;
