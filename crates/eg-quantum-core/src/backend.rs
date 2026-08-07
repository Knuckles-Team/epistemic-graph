//! `QuantumBackend` — the vendor-neutral execution trait every simulator or hardware
//! provider implements. This crate defines the trait and its capability vocabulary
//! only; it ships ZERO implementations. Backends are swappable by construction: the
//! planner (`planner.rs`) never names a concrete vendor, only a [`BackendFamily`],
//! and a caller registers whatever [`BackendDescriptor`]s are actually available at
//! runtime (Q3 QuEST-ffi, Q4 pure-Rust sv-cpu/stabilizer/sv-gpu/mps/trajectory, Q10
//! hardware providers — all future lanes, all behind their OWN default-off cargo
//! features on the root `epistemic-graph` package, never a hard dependency of this
//! crate).

use crate::ir::QuantumProgram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// A concrete backend instance's identifier, e.g. `"stabilizer"`, `"sv-cpu"`,
/// `"quest-ffi"`, `"hardware-ionq"`. Free-form (not a closed enum) because the set of
/// concrete backends is open — new ones register at runtime without this crate
/// changing. Pattern-matching on WHICH KIND of backend an id names belongs to
/// [`BackendFamily`], not this type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BackendId(pub String);

impl fmt::Display for BackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for BackendId {
    fn from(s: &str) -> Self {
        BackendId(s.to_string())
    }
}

/// The closed set of execution formalisms/placements the planner reasons about. A
/// concrete [`BackendId`] always belongs to exactly one family; several concrete ids
/// may share a family (e.g. `"sv-cpu"` and a future `"sv-cpu-simd"` are both
/// [`BackendFamily::StatevectorCpu`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendFamily {
    Stabilizer,
    StatevectorCpu,
    StatevectorGpu,
    DensityMatrixCpu,
    DensityMatrixGpu,
    MatrixProductState,
    Trajectory,
    /// The QuEST C/OpenMP/MPI/CUDA engine behind FFI (Q3) — its own family because it
    /// spans statevector AND density-matrix formalisms internally and the planner
    /// treats "route to QuEST" as one placement decision (R4), not a formalism choice.
    QuestFfi,
    /// A real QPU or cloud-hosted quantum hardware provider (Q10). Always
    /// non-exact (see `result.rs`) and always `is_exact: false` in its capabilities.
    Hardware,
}

impl BackendFamily {
    pub fn is_statevector_like(self) -> bool {
        matches!(
            self,
            BackendFamily::StatevectorCpu | BackendFamily::StatevectorGpu
        )
    }

    pub fn is_density_matrix_like(self) -> bool {
        matches!(
            self,
            BackendFamily::DensityMatrixCpu | BackendFamily::DensityMatrixGpu
        )
    }

    pub fn is_gpu(self) -> bool {
        matches!(
            self,
            BackendFamily::StatevectorGpu | BackendFamily::DensityMatrixGpu
        )
    }
}

/// Capability flags a backend advertises. The planner and any governance/quota layer
/// (Q8/Q9) reason ONLY over these flags plus [`BackendFamily`] — never over a vendor
/// name or a `#[cfg(feature = "...")]` compiled into this crate, which is how the
/// core stays vendor-neutral (CAPABILITY-flag design is the explicit Q0 charge, not
/// an incidental convenience).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub supports_density_matrix: bool,
    pub supports_distributed: bool,
    pub supports_noise: bool,
    pub supports_gpu: bool,
    pub supports_mps: bool,
    pub supports_stabilizer: bool,
    /// Whether a run on this backend under noiseless, unbounded-precision conditions
    /// yields a mathematically exact result (e.g. a statevector or stabilizer
    /// simulator: yes; a trajectory/sampling backend or real hardware: no, even with
    /// noise off, because shot noise / hardware imprecision still applies). This flag
    /// describes the BACKEND's ceiling; a given `QuantumResult.exact()` additionally
    /// depends on how it was actually run (see `result.rs` — noise/sampling requested
    /// at run time forces `exact: false` regardless of this flag).
    pub is_exact_capable: bool,
    /// `None` = no known qubit ceiling (e.g. a distributed backend bounded only by
    /// cluster memory, not expressible as a fixed constant here).
    pub max_qubits_statevector: Option<u32>,
    pub max_qubits_density_matrix: Option<u32>,
    pub requires_hardware: bool,
}

/// What the planner actually selects among: a concrete backend's identity, family,
/// and capabilities, as advertised by whatever registered the backend (a future
/// backend registry is out of Q0 scope — callers pass a `&[BackendDescriptor]`
/// directly to `planner::select_backend`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendDescriptor {
    pub id: BackendId,
    pub family: BackendFamily,
    pub capabilities: BackendCapabilities,
}

/// Options controlling one execution. `parameter_bindings` resolves every
/// `ParamValue::Symbol` the submitted `QuantumProgram` references (see `ir.rs`); a
/// backend implementation is responsible for rejecting an unbound symbol.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunOptions {
    pub shots: Option<u64>,
    pub seed: Option<u64>,
    pub noise_model_id: Option<String>,
    #[serde(default)]
    pub parameter_bindings: BTreeMap<String, f64>,
    pub timeout_ms: Option<u64>,
}

/// An opaque, backend-assigned handle to a submitted job. Callers must not assume
/// anything about its internal structure or ordering across backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobHandle(pub u64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend {0} does not support this operation")]
    Unsupported(BackendId),
    #[error("circuit rejected: {0}")]
    InvalidProgram(String),
    #[error("execution failed: {0}")]
    Execution(String),
    #[error("job was cancelled")]
    Cancelled,
    #[error("unknown job handle")]
    UnknownJob,
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),
}

/// The vendor-neutral execution contract. Every method is synchronous-signature but
/// `submit`/`poll`/`cancel` are explicitly split from the convenience `run` so a real
/// backend (QuEST-ffi worker pool, a distributed job-plane submission, a cloud
/// hardware queue) can implement genuine async submit-then-poll semantics; only a
/// simple in-process backend need make `run` synchronous end-to-end.
///
/// Deliberately NOT `dyn`-unsafe: every method takes `&self` and returns owned/Result
/// values, so `Box<dyn QuantumBackend>` / `Arc<dyn QuantumBackend>` both work, which
/// is how a future backend registry will hold a heterogeneous set of backends.
pub trait QuantumBackend: Send + Sync {
    fn backend_id(&self) -> BackendId;
    fn family(&self) -> BackendFamily;
    fn capabilities(&self) -> BackendCapabilities;

    /// Submit a program for execution; returns immediately with a handle to poll.
    fn submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<JobHandle, BackendError>;

    /// Poll a previously submitted job's status.
    fn poll(&self, job: JobHandle) -> Result<JobStatus, BackendError>;

    /// Retrieve the result of a `Completed` job. Calling this before completion, or
    /// on an unknown/cancelled/failed handle, is an error rather than a blocking
    /// wait — callers that want blocking semantics use `run`.
    fn result(&self, job: JobHandle) -> Result<crate::result::QuantumResult, BackendError>;

    /// Best-effort cancellation. A backend that cannot cancel in-flight work (e.g. a
    /// hardware queue past a point of no return) should still transition the job to
    /// `Cancelled`-or-terminal state and return `Ok(())` once it can no longer be
    /// polled meaningfully, per its own documentation.
    fn cancel(&self, job: JobHandle) -> Result<(), BackendError>;

    /// Convenience synchronous run: submit and block until terminal, per this
    /// backend's OWN blocking strategy (never a fixed-interval busy-loop baked into
    /// this trait — that anti-pattern belongs to no shared default here). No default
    /// body: every backend must state how it blocks.
    fn run(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<crate::result::QuantumResult, BackendError>;
}
