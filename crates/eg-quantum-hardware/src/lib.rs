//! `eg-quantum-hardware` -- optional third-party quantum hardware/cloud
//! [`eg_quantum_core::backend::QuantumBackend`] adapters (register `D-QN-1`, lane
//! Q10; see `plans/au-eg-program/program/quantum-external-providers.md` ss1.3).
//!
//! # What this crate is, and is not
//!
//! This crate wires IBM Quantum, Amazon Braket, and Azure Quantum in BEHIND the
//! existing, vendor-neutral `QuantumBackend` trait -- never as a front door. The
//! charter this crate implements is explicit that "a third-party Qiskit MCP server
//! cannot make [planner rule R1's Clifford-vs-statevector] choice for us," so nothing
//! here participates in circuit execution decisions except by being one more
//! [`eg_quantum_core::backend::BackendDescriptor`] the planner may or may not select.
//! `eg-quantum-sim` remains the default backend; these adapters exist for
//! validation, noise studies, and workloads exceeding local capacity.
//!
//! Every backend in this crate:
//! - reports [`eg_quantum_core::backend::BackendFamily::Hardware`];
//! - NEVER constructs an exact `QuantumResult` -- always
//!   [`eg_quantum_core::result::QuantumResult::new_inexact`], even for a provider's
//!   cloud-simulator SKU (SV1/DM1/TN1, `ibmq_qasm_simulator`, Azure's simulator
//!   targets), which are mathematically exact-capable in principle but are
//!   conservatively treated as non-exact here -- Q10's charter is "adapters for
//!   validation/noise studies/overflow capacity," not a second source of hard
//!   constraints, and the Q0 contract's load-bearing rule (only `exact: true` may
//!   become a `HardConstraint`) is safer with one unambiguous rule ("the whole
//!   `Hardware` family is never exact") than a finer-grained per-target-type
//!   distinction this lane has no way to independently verify;
//! - enforces its own rolling-window budget via [`quota`] BEFORE submitting, and
//!   refuses with `BackendError::ResourceLimit` (carrying the exhausted budget's
//!   numbers) rather than discovering exhaustion after a network round trip;
//! - resolves credentials via [`credentials`], which reads named environment
//!   variables ONLY (see that module's docs for why this is the correct OpenBao
//!   integration point, matching this repo's existing `S3_SECRET_KEY_ENV`/
//!   `KVCACHE_TOKEN_ENV` convention) -- never a literal default, never committed;
//! - is entirely feature-gated (`ibm`/`braket`/`azure`, each implying
//!   `reqwest-transport`), all OFF by default, and the root `epistemic-graph`
//!   package's `quantum-hardware` feature is the ONLY thing that turns any of them
//!   on -- a default build of this workspace links none of this crate's network
//!   code.
//!
//! # What is deliberately NOT done here (scope boundary)
//!
//! Translating `eg_quantum_core::ir::QuantumProgram` into each provider's native wire
//! format (Qiskit Runtime's PUB format, Braket's OpenQASM3 task input, Azure
//! Quantum's job input) is NOT implemented here. That translation's natural
//! foundation is Q2's OpenQASM 2.0 export (owned by the concurrently-landing
//! `w6-quantum-q2-qasm` lane, which this lane must not touch or duplicate). Every
//! adapter's `submit()` instead builds a minimal, provider-shaped IDENTITY envelope
//! (`circuit_hash`, `n_qubits`, shot count -- no gate-level payload) and sends it as
//! the request body; against a real provider this would be cleanly REJECTED as a
//! malformed circuit rather than silently misinterpreted, while still exercising
//! this crate's real, load-bearing machinery end to end: auth resolution,
//! SigV4/OAuth transport, quota enforcement, and the submit/poll/result/cancel job
//! state machine. A follow-up change only needs to replace the envelope's body
//! construction with a real encoder once Q2 lands, without touching any of this
//! crate's trait-boundary/quota/credential machinery.

pub mod credentials;
pub mod error;
pub mod quota;
pub mod transport;

#[cfg(feature = "sigv4")]
pub mod sigv4;

#[cfg(feature = "ibm")]
pub mod ibm;

#[cfg(feature = "braket")]
pub mod braket;

#[cfg(feature = "azure")]
pub mod azure;
