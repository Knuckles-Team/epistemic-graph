//! GPU-accelerable tensor-op dispatch seam (CONCEPT:EG-3.5 seam / EG-3.6 CUDA).
//!
//! The same seam pattern as `eg-ann::distance`, for the tensor elementwise kernel — the
//! dense, embarrassingly-parallel map over a buffer that is the natural GPU offload for the
//! array modality (CONCEPT:EG-085). [`elementwise_dispatch`] runs a scalar op over an `f64`
//! buffer on the ACTIVE backend:
//!
//!   * [`CpuBackend`] — the pure-Rust map, ALWAYS compiled in and the fallback.
//!   * [`cuda::CudaBackend`] — a REAL CUDA backend (feature `gpu-cuda`) that ships the
//!     buffer to the device and launches one thread per element; when no device is present
//!     [`active_backend`] returns the CPU backend, so a `gpu-cuda` binary is fully functional
//!     on a GPU-less host.
//!
//! Pi contract: the CPU path needs no feature; `gpu`/`gpu-cuda` are OUT of `pi`, and
//! `cudarc` links only under `gpu-cuda` (dlopen at runtime — no CUDA toolkit to build).

use crate::tensor::ElementwiseOp;

/// Integer op code shared by the CPU map and the CUDA kernel (CONCEPT:EG-3.6).
#[inline]
pub fn op_code(op: ElementwiseOp) -> i32 {
    match op {
        ElementwiseOp::Add => 0,
        ElementwiseOp::Sub => 1,
        ElementwiseOp::Mul => 2,
        ElementwiseOp::Div => 3,
    }
}

/// A backend that applies a scalar op to every element of an `f64` buffer (CONCEPT:EG-3.5).
/// Every backend MUST agree with the CPU backend so GPU/CPU tensor results are identical.
pub trait TensorBackend: Send + Sync {
    /// Stable backend name (`"cpu"`, `"cuda"`).
    fn name(&self) -> &'static str;
    /// `out[i] = op(data[i], scalar)` for every `i`.
    fn elementwise(&self, data: &[f64], op: ElementwiseOp, scalar: f64) -> Vec<f64>;
}

/// The always-compiled pure-Rust CPU backend (CONCEPT:EG-3.5).
#[derive(Debug, Default, Clone, Copy)]
pub struct CpuBackend;

impl TensorBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }
    fn elementwise(&self, data: &[f64], op: ElementwiseOp, scalar: f64) -> Vec<f64> {
        data.iter()
            .map(|&x| match op {
                ElementwiseOp::Add => x + scalar,
                ElementwiseOp::Sub => x - scalar,
                ElementwiseOp::Mul => x * scalar,
                ElementwiseOp::Div => x / scalar,
            })
            .collect()
    }
}

/// Apply a scalar op over `data` on the ACTIVE backend (CONCEPT:EG-3.5) — the single call
/// site `Tensor::elementwise` uses; the device choice is invisible to it.
pub fn elementwise_dispatch(data: &[f64], op: ElementwiseOp, scalar: f64) -> Vec<f64> {
    active_backend().elementwise(data, op, scalar)
}

/// The active tensor backend (CONCEPT:EG-3.5): CUDA when compiled + a device is present,
/// else CPU.
pub fn active_backend() -> &'static dyn TensorBackend {
    #[cfg(feature = "gpu-cuda")]
    {
        if let Some(b) = cuda::backend() {
            return b;
        }
    }
    static CPU: CpuBackend = CpuBackend;
    &CPU
}

/// The active backend's name (`"cpu"`/`"cuda"`).
pub fn active_backend_name() -> &'static str {
    active_backend().name()
}

// ── CONCEPT:EG-3.6 — the real CUDA tensor backend ────────────────────────────

#[cfg(feature = "gpu-cuda")]
pub mod cuda {
    //! REAL CUDA elementwise backend (CONCEPT:EG-3.6), NVRTC-compiled at first use. On any
    //! device/compile/launch failure the dispatch falls back to CPU.
    use super::{op_code, ElementwiseOp, TensorBackend};
    use std::sync::{Arc, OnceLock};

    use cudarc::driver::{CudaContext, CudaFunction, LaunchConfig, PushKernelArg};

    /// The elementwise kernel, CUDA-C (CONCEPT:EG-3.6). One thread per element; the four op
    /// branches match [`super::CpuBackend::elementwise`] exactly (add/sub/mul/div, f64).
    const KERNEL_SRC: &str = r#"
extern "C" __global__ void elementwise(
        const double* data, double* out, const double scalar, const int n, const int op) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    double x = data[i];
    if (op == 0) out[i] = x + scalar;
    else if (op == 1) out[i] = x - scalar;
    else if (op == 2) out[i] = x * scalar;
    else out[i] = x / scalar;
}
"#;

    /// The initialised CUDA backend (context + compiled kernel).
    pub struct CudaBackend {
        ctx: Arc<CudaContext>,
        func: CudaFunction,
    }

    impl CudaBackend {
        fn init(ordinal: usize) -> Result<Self, String> {
            let ctx = CudaContext::new(ordinal).map_err(|e| format!("cuda ctx: {e:?}"))?;
            let ptx =
                cudarc::nvrtc::compile_ptx(KERNEL_SRC).map_err(|e| format!("nvrtc: {e:?}"))?;
            let module = ctx
                .load_module(ptx)
                .map_err(|e| format!("load_module: {e:?}"))?;
            let func = module
                .load_function("elementwise")
                .map_err(|e| format!("load_function: {e:?}"))?;
            Ok(Self { ctx, func })
        }

        fn try_run(
            &self,
            data: &[f64],
            op: ElementwiseOp,
            scalar: f64,
        ) -> Result<Vec<f64>, String> {
            if data.is_empty() {
                return Ok(Vec::new());
            }
            let n = data.len();
            let stream = self.ctx.default_stream();
            let d_in = stream
                .memcpy_stod(data)
                .map_err(|e| format!("htod: {e:?}"))?;
            let mut d_out = stream
                .alloc_zeros::<f64>(n)
                .map_err(|e| format!("alloc: {e:?}"))?;
            let n_i32 = n as i32;
            let op_i32 = op_code(op);
            let cfg = LaunchConfig::for_num_elems(n as u32);
            let mut builder = stream.launch_builder(&self.func);
            builder
                .arg(&d_in)
                .arg(&mut d_out)
                .arg(&scalar)
                .arg(&n_i32)
                .arg(&op_i32);
            unsafe { builder.launch(cfg) }.map_err(|e| format!("launch: {e:?}"))?;
            stream
                .memcpy_dtov(&d_out)
                .map_err(|e| format!("dtoh: {e:?}"))
        }
    }

    impl TensorBackend for CudaBackend {
        fn name(&self) -> &'static str {
            "cuda"
        }
        fn elementwise(&self, data: &[f64], op: ElementwiseOp, scalar: f64) -> Vec<f64> {
            self.try_run(data, op, scalar)
                .unwrap_or_else(|_| super::CpuBackend.elementwise(data, op, scalar))
        }
    }

    /// The process-global CUDA backend, `Some` only if a device initialised (CONCEPT:EG-3.6).
    pub fn backend() -> Option<&'static dyn TensorBackend> {
        static B: OnceLock<Option<CudaBackend>> = OnceLock::new();
        B.get_or_init(|| {
            let ordinal = std::env::var("EPISTEMIC_GRAPH_TENSOR_DEVICE")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            // cudarc's `dynamic-loading` PANICS (not `Err`) when libcuda cannot be dlopen'd
            // on a GPU-less host — catch the unwind (silenced hook) to honor the CPU-fallback
            // contract: no device ⇒ `None` ⇒ CPU backend.
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let res = std::panic::catch_unwind(|| CudaBackend::init(ordinal));
            std::panic::set_hook(prev);
            match res {
                Ok(Ok(b)) => Some(b),
                _ => None,
            }
        })
        .as_ref()
        .map(|b| b as &dyn TensorBackend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg3_5_cpu_elementwise_matches_ops() {
        let data = vec![1.0, 2.0, 3.0];
        assert_eq!(
            CpuBackend.elementwise(&data, ElementwiseOp::Mul, 2.0),
            vec![2.0, 4.0, 6.0]
        );
        assert_eq!(
            CpuBackend.elementwise(&data, ElementwiseOp::Add, 10.0),
            vec![11.0, 12.0, 13.0]
        );
    }

    #[test]
    fn eg3_5_dispatch_is_usable_and_named() {
        let name = active_backend_name();
        assert!(name == "cpu" || name == "cuda");
        assert_eq!(
            elementwise_dispatch(&[4.0, 8.0], ElementwiseOp::Div, 2.0),
            vec![2.0, 4.0]
        );
    }
}
