//! GPU-accelerable batch-distance dispatch seam (CONCEPT:EG-KG.compute.gpu-distance-seam seam / EG-327 CUDA).
//!
//! Vector search is dominated by ONE kernel: score a query against a large batch of
//! candidate vectors under a [`Metric`]. That is embarrassingly parallel and the natural
//! GPU offload target. This module factors that kernel behind a [`DistanceBackend`] trait
//! so the compute device is a swappable backend WITHOUT touching the index code:
//!
//!   * [`CpuBackend`] — the pure-Rust CPU implementation, ALWAYS compiled in, and the
//!     fallback whenever a GPU is absent. It reuses the exact [`Metric::distance`] scalar
//!     the rest of the crate uses, so results are byte-for-byte the CPU ground truth.
//!   * [`cuda::CudaBackend`] — a REAL CUDA backend (feature `gpu-cuda`) that ships the
//!     batch-distance kernel to the GPU via `cudarc`. It is selected only when the feature
//!     is built AND a device initialises; otherwise [`active_backend`] returns the CPU
//!     backend, so a `gpu-cuda` binary on a GPU-less host still works (CPU fallback).
//!
//! Pi contract: the CPU path needs NO feature; `gpu`/`gpu-cuda` are OUT of `pi`/`default`
//! and `cudarc` links only under `gpu-cuda` (and even then dlopen's libcuda at runtime, so
//! the build needs no CUDA toolkit). A default/pi build links no accelerator dep.

use crate::flat::Metric;

/// A backend that scores a query against a contiguous batch of `n` row-major vectors
/// (CONCEPT:EG-KG.compute.gpu-distance-seam). `rows_flat` is `n * dim` f32 (row `i` = `rows_flat[i*dim..(i+1)*dim]`);
/// the returned `Vec<f32>` has length `n`, entry `i` = `metric.distance(query, row_i)`
/// under the crate's "smaller = nearer" convention. Every backend MUST agree with the CPU
/// backend to within floating-point tolerance so GPU and CPU results are interchangeable.
pub trait DistanceBackend: Send + Sync {
    /// Stable backend name for logs/metrics/tests (`"cpu"`, `"cuda"`).
    fn name(&self) -> &'static str;

    /// Score `query` (`dim` f32) against every row of `rows_flat` (`n * dim` f32).
    fn batch_distance(
        &self,
        query: &[f32],
        rows_flat: &[f32],
        dim: usize,
        metric: Metric,
    ) -> Vec<f32>;
}

/// Integer metric code shared by the CPU scalar path and the CUDA kernel (CONCEPT:EG-KG.backend.real-cuda-tensor-backend)
/// — keeps the two implementations in lockstep on which formula each `Metric` maps to.
#[inline]
pub fn metric_code(metric: Metric) -> i32 {
    match metric {
        Metric::L2 => 0,
        Metric::Cosine => 1,
        Metric::InnerProduct => 2,
    }
}

/// The always-compiled pure-Rust CPU backend (CONCEPT:EG-KG.compute.gpu-distance-seam).
#[derive(Debug, Default, Clone, Copy)]
pub struct CpuBackend;

impl DistanceBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn batch_distance(
        &self,
        query: &[f32],
        rows_flat: &[f32],
        dim: usize,
        metric: Metric,
    ) -> Vec<f32> {
        debug_assert_eq!(query.len(), dim, "query length must equal dim");
        if dim == 0 {
            return Vec::new();
        }
        rows_flat
            .chunks_exact(dim)
            .map(|row| metric.distance(query, row))
            .collect()
    }
}

/// Score `query` against `rows_flat` on the ACTIVE backend (CONCEPT:EG-KG.compute.gpu-distance-seam) — the GPU
/// backend when `gpu-cuda` is built and a device is present, else the CPU backend. This is
/// the single call site index code uses; the device choice is invisible to it.
pub fn batch_distances(query: &[f32], rows_flat: &[f32], dim: usize, metric: Metric) -> Vec<f32> {
    active_backend().batch_distance(query, rows_flat, dim, metric)
}

/// The active backend (CONCEPT:EG-KG.compute.gpu-distance-seam). Selects the CUDA backend when it is compiled AND a
/// device initialises; otherwise the CPU backend. Built once and cached.
pub fn active_backend() -> &'static dyn DistanceBackend {
    #[cfg(feature = "gpu-cuda")]
    {
        if let Some(b) = cuda::backend() {
            return b;
        }
    }
    static CPU: CpuBackend = CpuBackend;
    &CPU
}

/// The active backend's name (`"cpu"`/`"cuda"`) for observability/tests.
pub fn active_backend_name() -> &'static str {
    active_backend().name()
}

// ── CONCEPT:EG-KG.backend.real-cuda-tensor-backend — the real CUDA backend ───────────────────────────────────

#[cfg(feature = "gpu-cuda")]
pub mod cuda {
    //! REAL CUDA distance backend (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). Compiles the batch-distance CUDA-C
    //! kernel with NVRTC at first use, ships the query + row matrix to the device, launches
    //! one thread per row, and copies the scores back. On ANY device/compile/launch failure
    //! it returns `None` so [`super::active_backend`] falls back to CPU — a `gpu-cuda`
    //! binary on a GPU-less host is fully functional.
    use super::{metric_code, DistanceBackend, Metric};
    use std::sync::{Arc, OnceLock};

    use cudarc::driver::{CudaContext, CudaFunction, LaunchConfig, PushKernelArg};

    /// The batch-distance kernel, CUDA-C (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). One thread scores one row; the
    /// three metric branches match [`Metric::distance`] EXACTLY (L2 squared, cosine distance
    /// `1 - cos` with `1.0` for a zero-norm vector, negated inner product) so GPU results
    /// equal the CPU ground truth.
    const KERNEL_SRC: &str = r#"
extern "C" __global__ void batch_distance(
        const float* query, const float* rows, float* out,
        const int n, const int dim, const int metric) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    const float* r = rows + (size_t)i * dim;
    if (metric == 0) {
        float s = 0.0f;
        for (int d = 0; d < dim; d++) { float diff = query[d] - r[d]; s += diff * diff; }
        out[i] = s;
    } else if (metric == 1) {
        float dot = 0.0f, na = 0.0f, nb = 0.0f;
        for (int d = 0; d < dim; d++) { dot += query[d]*r[d]; na += query[d]*query[d]; nb += r[d]*r[d]; }
        float denom = sqrtf(na) * sqrtf(nb);
        out[i] = denom <= 0.0f ? 1.0f : (1.0f - dot / denom);
    } else {
        float dot = 0.0f;
        for (int d = 0; d < dim; d++) dot += query[d] * r[d];
        out[i] = -dot;
    }
}
"#;

    /// The initialised CUDA backend (context + compiled kernel).
    pub struct CudaBackend {
        ctx: Arc<CudaContext>,
        func: CudaFunction,
    }

    impl CudaBackend {
        /// Initialise device `ordinal`, NVRTC-compile the kernel, and load it (CONCEPT:EG-KG.backend.real-cuda-tensor-backend).
        /// `Err` on any failure (no driver / no device / compile error) — the caller degrades
        /// to CPU.
        fn init(ordinal: usize) -> Result<Self, String> {
            let ctx = CudaContext::new(ordinal).map_err(|e| format!("cuda ctx: {e:?}"))?;
            let ptx =
                cudarc::nvrtc::compile_ptx(KERNEL_SRC).map_err(|e| format!("nvrtc: {e:?}"))?;
            let module = ctx
                .load_module(ptx)
                .map_err(|e| format!("load_module: {e:?}"))?;
            let func = module
                .load_function("batch_distance")
                .map_err(|e| format!("load_function: {e:?}"))?;
            Ok(Self { ctx, func })
        }
    }

    impl DistanceBackend for CudaBackend {
        fn name(&self) -> &'static str {
            "cuda"
        }

        fn batch_distance(
            &self,
            query: &[f32],
            rows_flat: &[f32],
            dim: usize,
            metric: Metric,
        ) -> Vec<f32> {
            match self.try_batch(query, rows_flat, dim, metric) {
                Ok(out) => out,
                // A transient launch/copy failure degrades to CPU for THIS call rather than
                // returning wrong/empty scores — correctness over acceleration.
                Err(e) => {
                    tracing::warn!("cuda batch_distance failed ({e}); CPU fallback");
                    super::CpuBackend.batch_distance(query, rows_flat, dim, metric)
                }
            }
        }
    }

    impl CudaBackend {
        fn try_batch(
            &self,
            query: &[f32],
            rows_flat: &[f32],
            dim: usize,
            metric: Metric,
        ) -> Result<Vec<f32>, String> {
            if dim == 0 || rows_flat.is_empty() {
                return Ok(Vec::new());
            }
            let n = rows_flat.len() / dim;
            let stream = self.ctx.default_stream();
            let d_query = stream
                .memcpy_stod(query)
                .map_err(|e| format!("htod query: {e:?}"))?;
            let d_rows = stream
                .memcpy_stod(rows_flat)
                .map_err(|e| format!("htod rows: {e:?}"))?;
            let mut d_out = stream
                .alloc_zeros::<f32>(n)
                .map_err(|e| format!("alloc out: {e:?}"))?;
            let n_i32 = n as i32;
            let dim_i32 = dim as i32;
            let metric_i32 = metric_code(metric);
            let cfg = LaunchConfig::for_num_elems(n as u32);
            let mut builder = stream.launch_builder(&self.func);
            builder
                .arg(&d_query)
                .arg(&d_rows)
                .arg(&mut d_out)
                .arg(&n_i32)
                .arg(&dim_i32)
                .arg(&metric_i32);
            unsafe { builder.launch(cfg) }.map_err(|e| format!("launch: {e:?}"))?;
            stream
                .memcpy_dtov(&d_out)
                .map_err(|e| format!("dtoh out: {e:?}"))
        }
    }

    /// The process-global CUDA backend, `Some` only if a device initialised (CONCEPT:EG-KG.backend.real-cuda-tensor-backend).
    /// `EPISTEMIC_GRAPH_ANN_DEVICE` selects the ordinal (default 0). Cached, so device init +
    /// kernel compile happen once.
    pub fn backend() -> Option<&'static dyn DistanceBackend> {
        static B: OnceLock<Option<CudaBackend>> = OnceLock::new();
        B.get_or_init(|| {
            let ordinal = std::env::var("EPISTEMIC_GRAPH_ANN_DEVICE")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            // cudarc's `dynamic-loading` PANICS (does not return `Err`) when libcuda cannot
            // be dlopen'd — i.e. on any GPU-less host. Catch that unwind (under a silenced
            // hook) so the CPU-fallback contract holds: no device ⇒ `None` ⇒ CPU backend.
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let res = std::panic::catch_unwind(|| CudaBackend::init(ordinal));
            std::panic::set_hook(prev);
            match res {
                Ok(Ok(b)) => Some(b),
                Ok(Err(e)) => {
                    tracing::info!("CUDA distance backend unavailable ({e}); using CPU");
                    None
                }
                Err(_) => {
                    tracing::info!("CUDA driver not loadable; using CPU distance backend");
                    None
                }
            }
        })
        .as_ref()
        .map(|b| b as &dyn DistanceBackend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg3_5_cpu_backend_batch_matches_scalar_metric() {
        let dim = 3;
        let rows = vec![
            0.0, 0.0, 0.0, // row 0
            1.0, 0.0, 0.0, // row 1
            0.0, 2.0, 0.0, // row 2
        ];
        let q = vec![1.0, 1.0, 0.0];
        for metric in [Metric::L2, Metric::Cosine, Metric::InnerProduct] {
            let batched = CpuBackend.batch_distance(&q, &rows, dim, metric);
            assert_eq!(batched.len(), 3);
            for (i, row) in rows.chunks_exact(dim).enumerate() {
                let scalar = metric.distance(&q, row);
                assert!(
                    (batched[i] - scalar).abs() < 1e-6,
                    "batch != scalar for {metric:?} row {i}"
                );
            }
        }
    }

    #[test]
    fn eg3_5_active_backend_is_usable_and_named() {
        // Without gpu-cuda (or with no GPU) the active backend is the CPU fallback.
        let name = active_backend_name();
        assert!(name == "cpu" || name == "cuda", "unexpected backend {name}");
        let d = batch_distances(&[1.0, 0.0], &[1.0, 0.0, 0.0, 1.0], 2, Metric::L2);
        assert_eq!(d.len(), 2);
        assert!((d[0]).abs() < 1e-6, "identical vector distance ~0");
    }

    #[test]
    fn eg3_5_dispatch_and_flat_search_agree() {
        // The dispatch seam yields the SAME ordering the FlatIndex scalar search does.
        use crate::flat::FlatIndex;
        let dim = 4;
        let mut idx = FlatIndex::new(dim);
        idx.add(&[
            (1, vec![1.0, 0.0, 0.0, 0.0]),
            (2, vec![0.0, 1.0, 0.0, 0.0]),
            (3, vec![0.9, 0.1, 0.0, 0.0]),
        ]);
        let q = vec![1.0, 0.0, 0.0, 0.0];
        let dists = batch_distances(&q, &idx.vectors, dim, Metric::L2);
        // Row 0 (id 1) is the exact match → smallest distance.
        let nearest = dists
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .unwrap()
            .0;
        assert_eq!(nearest, 0, "batch distances rank the exact match first");
        assert_eq!(idx.search(&q, 1, Metric::L2)[0].id, 1);
    }

    /// GPU↔CPU parity (CONCEPT:EG-KG.compute.tensor-gpu-distance). When a CUDA device is present, the real CUDA
    /// batch-distance kernel MUST match the CPU ground truth to within f32 tolerance
    /// across every metric; when no device is available `cuda::backend()` is `None` and
    /// the test SKIPS cleanly. So it is a no-op in GPU-less CI yet auto-validates the
    /// EG-327 kernel wherever a compatible CUDA device exists without breaking CI. Only
    /// compiled under `--features gpu-cuda` (the `cuda` module is gated on that feature).
    #[cfg(feature = "gpu-cuda")]
    #[test]
    fn eg351_cuda_batch_distance_matches_cpu_ground_truth() {
        let Some(gpu) = cuda::backend() else {
            eprintln!("SKIP eg351_cuda_batch_distance: no CUDA device present (CPU-only host)");
            return;
        };
        assert_eq!(gpu.name(), "cuda", "backend() returned a non-CUDA backend");

        // A non-trivial batch: several dims, a zero-norm row (exercises the cosine
        // zero-denominator branch), and mixed-sign values.
        let dim = 5;
        let rows: Vec<f32> = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, // zero-norm row → cosine == 1.0
            1.0, 2.0, 3.0, 4.0, 5.0, //
            -1.0, 0.5, 2.5, -3.0, 1.0, //
            9.0, -8.0, 7.0, -6.0, 5.0, //
            0.25, 0.25, 0.25, 0.25, 0.25, //
        ];
        let q = vec![0.3, -1.2, 2.0, 0.0, 4.4];

        for metric in [Metric::L2, Metric::Cosine, Metric::InnerProduct] {
            let cpu = CpuBackend.batch_distance(&q, &rows, dim, metric);
            let gpu_out = gpu.batch_distance(&q, &rows, dim, metric);
            assert_eq!(cpu.len(), gpu_out.len(), "length mismatch for {metric:?}");
            for (i, (c, g)) in cpu.iter().zip(gpu_out.iter()).enumerate() {
                // f32 accumulation on the GPU (FMA/rounding) can differ from the scalar
                // CPU path by a few ULP — assert bitwise-CLOSE with a relative tolerance.
                let tol = 1e-4_f32 * (1.0 + c.abs());
                assert!(
                    (c - g).abs() <= tol,
                    "GPU!=CPU for {metric:?} row {i}: cpu={c} gpu={g} (tol {tol})"
                );
            }
        }
    }
}
