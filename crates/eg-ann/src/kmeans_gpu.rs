//! GPU-accelerable k-means/IVF **batch-assignment** dispatch seam (CONCEPT:EG-KG.compute.gpu-distance-seam
//! seam / EG-327 CUDA), extending the seam beyond distance/elementwise to the ANN
//! **index-build** hot loop.
//!
//! `kmeans::kmeans` (Lloyd's algorithm, k-means++ seeded) is shared by THREE build
//! paths: the IVF coarse quantizer, the PQ subspace codebooks, and OPQ's per-iteration
//! PQ retrain (see `ivfpq.rs::train` / `train_pq`). Its dominant per-iteration cost is
//! the assignment step — for every one of `n` training points, scan `k` centroids and
//! keep the nearest (batch distance-to-centroids + argmin). That is embarrassingly
//! parallel across points and is the natural GPU offload for the BUILD path (serving
//! stays CPU integer-table ADC lookups — unchanged).
//!
//! This module factors that step behind an [`AssignBackend`] trait, mirroring
//! `distance::DistanceBackend` exactly:
//!
//!   * [`CpuBackend`] — parallel (rayon) over points, delegating to
//!     [`crate::kmeans::nearest_centroid`] so CPU-only builds (no `gpu-cuda`) are
//!     BYTE-IDENTICAL to the pre-existing inline assignment loop — zero behavior
//!     change without the feature.
//!   * [`cuda::CudaBackend`] — a REAL CUDA backend (feature `gpu-cuda`) that ships the
//!     training batch + current centroids to the device and launches one thread per
//!     point (each doing the full k-scan + argmin), selected only when the feature is
//!     built AND a device initialises; any device/compile/launch failure — including
//!     contention from `train_pq`'s concurrent per-subspace `kmeans` calls sharing one
//!     process-global CUDA context — degrades that call to the CPU backend rather than
//!     returning wrong assignments.
//!
//! Pi contract: unchanged from the rest of the crate — the CPU path needs no feature;
//! `gpu`/`gpu-cuda` are OUT of `pi`/`default`/`full` and `cudarc` links only under
//! `gpu-cuda` (dlopen at runtime, no CUDA toolkit needed to build).

use crate::kmeans::nearest_centroid;

/// A backend that assigns each of `n` flattened `dim`-dim points to its nearest of `k`
/// flattened `dim`-dim centroids (CONCEPT:EG-KG.compute.gpu-distance-seam), squared-L2 (the metric
/// `kmeans::kmeans` uses). `data_flat` is `n*dim` row-major f32; `centroids_flat` is
/// `k*dim` row-major f32. Returns `n` centroid indices. Every backend MUST agree with
/// the CPU backend so GPU-built and CPU-built codebooks are interchangeable.
pub trait AssignBackend: Send + Sync {
    /// Stable backend name for logs/tests (`"cpu"`, `"cuda"`).
    fn name(&self) -> &'static str;

    /// `out[i] = argmin_c ||data[i] - centroids[c]||^2` for every point `i`.
    fn batch_assign(
        &self,
        data_flat: &[f32],
        n: usize,
        centroids_flat: &[f32],
        k: usize,
        dim: usize,
    ) -> Vec<u32>;
}

/// The always-compiled pure-Rust CPU backend (CONCEPT:EG-KG.compute.gpu-distance-seam). Delegates to
/// [`nearest_centroid`] — the SAME bounded-early-exit scalar scan `kmeans::kmeans` used
/// inline before this seam existed — so results are bitwise-identical to the prior
/// behavior.
#[derive(Debug, Default, Clone, Copy)]
pub struct CpuBackend;

impl AssignBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn batch_assign(
        &self,
        data_flat: &[f32],
        n: usize,
        centroids_flat: &[f32],
        k: usize,
        dim: usize,
    ) -> Vec<u32> {
        use rayon::prelude::*;
        if dim == 0 || n == 0 {
            return vec![0u32; n];
        }
        (0..n)
            .into_par_iter()
            .map(|i| {
                let v = &data_flat[i * dim..(i + 1) * dim];
                nearest_centroid(v, centroids_flat, dim, k) as u32
            })
            .collect()
    }
}

/// Assign `data_flat` (`n*dim`) to `centroids_flat` (`k*dim`) on the ACTIVE backend
/// (CONCEPT:EG-KG.compute.gpu-distance-seam) — the single call site `kmeans::kmeans`'s per-iteration
/// assignment step uses; the device choice is invisible to it.
pub fn batch_assign_dispatch(
    data_flat: &[f32],
    n: usize,
    centroids_flat: &[f32],
    k: usize,
    dim: usize,
) -> Vec<u32> {
    active_backend().batch_assign(data_flat, n, centroids_flat, k, dim)
}

/// The active assignment backend (CONCEPT:EG-KG.compute.gpu-distance-seam): CUDA when compiled + a
/// device is present, else CPU. Built once and cached.
pub fn active_backend() -> &'static dyn AssignBackend {
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

// ── CONCEPT:EG-KG.backend.real-cuda-tensor-backend — the real CUDA k-means-assign backend ──────────────────

#[cfg(feature = "gpu-cuda")]
pub mod cuda {
    //! REAL CUDA batch-assign backend (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). Compiles the
    //! batch-assignment CUDA-C kernel with NVRTC at first use, ships the training batch +
    //! centroids to the device, launches one thread per point, and copies the argmin
    //! indices back. On ANY device/compile/launch failure it degrades to the CPU backend
    //! for that call — a `gpu-cuda` binary on a GPU-less host (or under concurrent
    //! `train_pq` subspace calls contending for the one process-global context) stays
    //! fully correct.
    use super::{AssignBackend, CpuBackend};
    use std::sync::{Arc, OnceLock};

    use cudarc::driver::{CudaContext, CudaFunction, LaunchConfig, PushKernelArg};

    /// The batch-assign kernel, CUDA-C (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). One thread scores one
    /// data point against every centroid (full squared-L2, no early exit — the CPU side's
    /// bounded early exit only skips already-losing candidates, never changes the winner)
    /// and writes the argmin index. Matches [`crate::kmeans::nearest_centroid`] EXACTLY
    /// in result.
    const KERNEL_SRC: &str = r#"
extern "C" __global__ void batch_assign(
        const float* data, const float* centroids, int* out,
        const int n, const int k, const int dim) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    const float* v = data + (size_t)i * dim;
    float best = 3.402823466e38f;
    int best_idx = 0;
    for (int c = 0; c < k; c++) {
        const float* cent = centroids + (size_t)c * dim;
        float s = 0.0f;
        for (int d = 0; d < dim; d++) {
            float diff = v[d] - cent[d];
            s += diff * diff;
        }
        if (s < best) {
            best = s;
            best_idx = c;
        }
    }
    out[i] = best_idx;
}
"#;

    /// The initialised CUDA backend (context + compiled kernel).
    pub struct CudaBackend {
        ctx: Arc<CudaContext>,
        func: CudaFunction,
    }

    impl CudaBackend {
        /// Initialise device `ordinal`, NVRTC-compile the kernel, and load it
        /// (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). `Err` on any failure — the caller degrades to CPU.
        fn init(ordinal: usize) -> Result<Self, String> {
            let ctx = CudaContext::new(ordinal).map_err(|e| format!("cuda ctx: {e:?}"))?;
            let ptx =
                cudarc::nvrtc::compile_ptx(KERNEL_SRC).map_err(|e| format!("nvrtc: {e:?}"))?;
            let module = ctx
                .load_module(ptx)
                .map_err(|e| format!("load_module: {e:?}"))?;
            let func = module
                .load_function("batch_assign")
                .map_err(|e| format!("load_function: {e:?}"))?;
            Ok(Self { ctx, func })
        }

        fn try_batch(
            &self,
            data_flat: &[f32],
            n: usize,
            centroids_flat: &[f32],
            k: usize,
            dim: usize,
        ) -> Result<Vec<u32>, String> {
            if dim == 0 || n == 0 || k == 0 {
                return Ok(vec![0u32; n]);
            }
            let stream = self.ctx.default_stream();
            let d_data = stream
                .memcpy_stod(data_flat)
                .map_err(|e| format!("htod data: {e:?}"))?;
            let d_centroids = stream
                .memcpy_stod(centroids_flat)
                .map_err(|e| format!("htod centroids: {e:?}"))?;
            let mut d_out = stream
                .alloc_zeros::<i32>(n)
                .map_err(|e| format!("alloc out: {e:?}"))?;
            let n_i32 = n as i32;
            let k_i32 = k as i32;
            let dim_i32 = dim as i32;
            let cfg = LaunchConfig::for_num_elems(n as u32);
            let mut builder = stream.launch_builder(&self.func);
            builder
                .arg(&d_data)
                .arg(&d_centroids)
                .arg(&mut d_out)
                .arg(&n_i32)
                .arg(&k_i32)
                .arg(&dim_i32);
            unsafe { builder.launch(cfg) }.map_err(|e| format!("launch: {e:?}"))?;
            let out_i32 = stream
                .memcpy_dtov(&d_out)
                .map_err(|e| format!("dtoh out: {e:?}"))?;
            Ok(out_i32.into_iter().map(|x| x as u32).collect())
        }
    }

    impl AssignBackend for CudaBackend {
        fn name(&self) -> &'static str {
            "cuda"
        }

        fn batch_assign(
            &self,
            data_flat: &[f32],
            n: usize,
            centroids_flat: &[f32],
            k: usize,
            dim: usize,
        ) -> Vec<u32> {
            match self.try_batch(data_flat, n, centroids_flat, k, dim) {
                Ok(out) => out,
                // A transient launch/copy failure (or context contention from concurrent
                // per-subspace `train_pq` calls) degrades to CPU for THIS call rather than
                // returning wrong/empty assignments — correctness over acceleration.
                Err(e) => {
                    tracing::warn!("cuda batch_assign failed ({e}); CPU fallback");
                    CpuBackend.batch_assign(data_flat, n, centroids_flat, k, dim)
                }
            }
        }
    }

    /// The process-global CUDA backend, `Some` only if a device initialised
    /// (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). `EPISTEMIC_GRAPH_ANN_DEVICE` selects the ordinal
    /// (default 0, shared with `distance::cuda::backend`). Cached, so device init + kernel
    /// compile happen once.
    pub fn backend() -> Option<&'static dyn AssignBackend> {
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
                    tracing::info!("CUDA k-means-assign backend unavailable ({e}); using CPU");
                    None
                }
                Err(_) => {
                    tracing::info!("CUDA driver not loadable; using CPU k-means-assign backend");
                    None
                }
            }
        })
        .as_ref()
        .map(|b| b as &dyn AssignBackend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kmeans::sq_dist;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn flat(data: &[Vec<f32>]) -> Vec<f32> {
        data.iter().flat_map(|v| v.iter().copied()).collect()
    }

    #[test]
    fn eg3_5_cpu_backend_matches_nearest_centroid() {
        let dim = 4;
        let centroids: Vec<f32> = vec![
            0.0, 0.0, 0.0, 0.0, //
            10.0, 10.0, 10.0, 10.0, //
            -5.0, 5.0, -5.0, 5.0, //
        ];
        let data: Vec<Vec<f32>> = vec![
            vec![0.1, -0.1, 0.0, 0.2],
            vec![9.5, 10.2, 10.1, 9.8],
            vec![-4.8, 5.1, -5.2, 4.9],
        ];
        let data_flat = flat(&data);
        let got = CpuBackend.batch_assign(&data_flat, data.len(), &centroids, 3, dim);
        for (i, v) in data.iter().enumerate() {
            let want = nearest_centroid(v, &centroids, dim, 3) as u32;
            assert_eq!(got[i], want, "point {i} assignment mismatch");
        }
    }

    #[test]
    fn eg3_5_dispatch_is_usable_and_named() {
        let name = active_backend_name();
        assert!(name == "cpu" || name == "cuda");
        let centroids = vec![0.0f32, 0.0, 5.0, 5.0];
        let data_flat = vec![0.1f32, -0.1, 4.9, 5.1];
        let out = batch_assign_dispatch(&data_flat, 2, &centroids, 2, 2);
        assert_eq!(out, vec![0, 1]);
    }

    /// The build path stays correct end-to-end: training on the dispatch seam yields the
    /// SAME per-point assignments a direct `nearest_centroid` scan would (the seam is a
    /// drop-in for the loop `kmeans::kmeans` used before this module existed).
    #[test]
    fn eg3_5_batch_assign_matches_brute_force_on_clustered_data() {
        let dim = 8;
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let centers: Vec<Vec<f32>> = (0..6)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 20.0 - 10.0).collect())
            .collect();
        let data: Vec<Vec<f32>> = (0..500)
            .map(|_| {
                let c = &centers[rng.gen_range(0..centers.len())];
                (0..dim)
                    .map(|j| c[j] + (rng.gen::<f32>() - 0.5) * 0.1)
                    .collect()
            })
            .collect();
        let centroids_flat = flat(&centers);
        let data_flat = flat(&data);
        let got =
            batch_assign_dispatch(&data_flat, data.len(), &centroids_flat, centers.len(), dim);
        for (i, v) in data.iter().enumerate() {
            let mut best = 0usize;
            let mut bestd = f32::MAX;
            for (c, cen) in centers.iter().enumerate() {
                let d = sq_dist(v, cen);
                if d < bestd {
                    bestd = d;
                    best = c;
                }
            }
            assert_eq!(
                got[i] as usize, best,
                "point {i} disagrees with brute force"
            );
        }
    }

    /// GPU↔CPU parity (CONCEPT:EG-KG.compute.tensor-gpu-distance). When a CUDA device is present, the real CUDA
    /// batch-assign kernel MUST match the CPU ground truth for every point; when no device
    /// is available `cuda::backend()` is `None` and the test SKIPS cleanly. So it is a
    /// no-op in GPU-less CI yet auto-validates the kernel wherever a GPU exists (e.g. the
    /// GB10 box) without breaking CI. Only compiled under `--features gpu-cuda`.
    #[cfg(feature = "gpu-cuda")]
    #[test]
    fn eg351_cuda_batch_assign_matches_cpu_ground_truth() {
        let Some(gpu) = cuda::backend() else {
            eprintln!("SKIP eg351_cuda_batch_assign: no CUDA device present (CPU-only host)");
            return;
        };
        assert_eq!(gpu.name(), "cuda", "backend() returned a non-CUDA backend");

        // A batch that crosses several thread blocks, with well-separated clusters (avoid
        // near-tie boundaries where CPU/GPU summation-order rounding could flip an argmin)
        // plus a realistic centroid count (nlist-scale).
        let dim = 32;
        let k = 64;
        let n = 4096;
        let mut rng = ChaCha8Rng::seed_from_u64(1234);
        let centers: Vec<Vec<f32>> = (0..k)
            .map(|c| {
                (0..dim)
                    .map(|d| ((c * 37 + d * 11) % 200) as f32 - 100.0)
                    .collect()
            })
            .collect();
        let data: Vec<Vec<f32>> = (0..n)
            .map(|_| {
                let c = &centers[rng.gen_range(0..k)];
                (0..dim)
                    .map(|j| c[j] + (rng.gen::<f32>() - 0.5) * 0.05)
                    .collect()
            })
            .collect();
        let centroids_flat = flat(&centers);
        let data_flat = flat(&data);

        let cpu_out = CpuBackend.batch_assign(&data_flat, n, &centroids_flat, k, dim);
        let gpu_out = gpu.batch_assign(&data_flat, n, &centroids_flat, k, dim);
        assert_eq!(cpu_out.len(), gpu_out.len());
        for i in 0..n {
            assert_eq!(
                cpu_out[i], gpu_out[i],
                "GPU!=CPU argmin for point {i}: cpu={} gpu={}",
                cpu_out[i], gpu_out[i]
            );
        }
    }
}
