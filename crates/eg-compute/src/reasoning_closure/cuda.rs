//! REAL CUDA transitive-join backend (CONCEPT:EG-KG.backend.real-cuda-tensor-backend).
//! Compiles a two-pass CSR-join kernel with NVRTC at first use and runs the
//! boolean-semiring join `{(x,z) | (x,y)∈left, (y,z)∈right}` on the device: pass 1
//! binary-searches each left edge's middle key into the sorted right relation to count
//! its matches; the host exclusive-scans the counts; pass 2 scatters each left edge's
//! `(x, z)` pairs. On ANY device/compile/launch failure it degrades to the CPU backend
//! for that call, so a `gpu-cuda` binary on a GPU-less host (or under context
//! contention) stays fully correct.
use super::{ClosureBackend, CpuBackend};
use std::sync::{Arc, OnceLock};

use cudarc::driver::{CudaContext, CudaFunction, LaunchConfig, PushKernelArg};

/// Two kernels, CUDA-C (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). `right` is
/// pre-sorted by its source key on the host; each left edge binary-searches that key
/// to find its match run `[lo, lo+cnt)` in the sorted right arrays. `count_matches`
/// fills `lo[]`/`cnt[]` (host scans `cnt` into offsets `off[]`); `scatter_pairs`
/// writes each left edge's `(x, z)` pairs at `off[i]`. Result matches the CPU
/// hash-join set exactly (only the emission ORDER differs; the driver dedups to a set).
const KERNEL_SRC: &str = r#"
extern "C" __device__ void bounds(
    const int* keys, int m, int key, int* lo_out, int* cnt_out) {
// First index with keys[idx] >= key.
int lo = 0, hi = m;
while (lo < hi) { int mid = (lo + hi) >> 1; if (keys[mid] < key) lo = mid + 1; else hi = mid; }
int start = lo;
// First index with keys[idx] > key.
hi = m; int lo2 = start;
while (lo2 < hi) { int mid = (lo2 + hi) >> 1; if (keys[mid] <= key) lo2 = mid + 1; else hi = mid; }
*lo_out = start;
*cnt_out = lo2 - start;
}

extern "C" __global__ void count_matches(
    const int* left_y, int n_left,
    const int* right_key, int m_right,
    int* lo, int* cnt) {
int i = blockIdx.x * blockDim.x + threadIdx.x;
if (i >= n_left) return;
int l, c;
bounds(right_key, m_right, left_y[i], &l, &c);
lo[i] = l;
cnt[i] = c;
}

extern "C" __global__ void scatter_pairs(
    const int* left_x, int n_left,
    const int* right_val,
    const int* lo, const int* off,
    int* out_x, int* out_z) {
int i = blockIdx.x * blockDim.x + threadIdx.x;
if (i >= n_left) return;
int base = off[i];
int l = lo[i];
int next = off[i + 1];
int c = next - base;
for (int j = 0; j < c; j++) {
    out_x[base + j] = left_x[i];
    out_z[base + j] = right_val[l + j];
}
}
"#;

/// The initialised CUDA backend (context + both compiled kernels).
pub struct CudaBackend {
    ctx: Arc<CudaContext>,
    count: CudaFunction,
    scatter: CudaFunction,
}

impl CudaBackend {
    fn init(ordinal: usize) -> Result<Self, String> {
        let ctx = CudaContext::new(ordinal).map_err(|e| format!("cuda ctx: {e:?}"))?;
        let ptx = cudarc::nvrtc::compile_ptx(KERNEL_SRC).map_err(|e| format!("nvrtc: {e:?}"))?;
        let module = ctx
            .load_module(ptx)
            .map_err(|e| format!("load_module: {e:?}"))?;
        let count = module
            .load_function("count_matches")
            .map_err(|e| format!("load count_matches: {e:?}"))?;
        let scatter = module
            .load_function("scatter_pairs")
            .map_err(|e| format!("load scatter_pairs: {e:?}"))?;
        Ok(Self {
            ctx,
            count,
            scatter,
        })
    }

    fn try_join(
        &self,
        left: &[(u32, u32)],
        right: &[(u32, u32)],
    ) -> Result<Vec<(u32, u32)>, String> {
        let n = left.len();
        if n == 0 || right.is_empty() {
            return Ok(Vec::new());
        }
        // Host: sort `right` by source key into parallel arrays for the CSR search.
        let mut right_sorted = right.to_vec();
        right_sorted.sort_unstable_by_key(|&(y, _)| y);
        let right_key: Vec<i32> = right_sorted.iter().map(|&(y, _)| y as i32).collect();
        let right_val: Vec<i32> = right_sorted.iter().map(|&(_, z)| z as i32).collect();
        let left_x: Vec<i32> = left.iter().map(|&(x, _)| x as i32).collect();
        let left_y: Vec<i32> = left.iter().map(|&(_, y)| y as i32).collect();

        let stream = self.ctx.default_stream();
        let d_left_y = stream
            .memcpy_stod(&left_y)
            .map_err(|e| format!("htod left_y: {e:?}"))?;
        let d_right_key = stream
            .memcpy_stod(&right_key)
            .map_err(|e| format!("htod right_key: {e:?}"))?;
        let mut d_lo = stream
            .alloc_zeros::<i32>(n)
            .map_err(|e| format!("alloc lo: {e:?}"))?;
        let mut d_cnt = stream
            .alloc_zeros::<i32>(n)
            .map_err(|e| format!("alloc cnt: {e:?}"))?;

        let n_i32 = n as i32;
        let m_i32 = right_sorted.len() as i32;
        let cfg = LaunchConfig::for_num_elems(n as u32);
        {
            let mut b = stream.launch_builder(&self.count);
            b.arg(&d_left_y)
                .arg(&n_i32)
                .arg(&d_right_key)
                .arg(&m_i32)
                .arg(&mut d_lo)
                .arg(&mut d_cnt);
            unsafe { b.launch(cfg) }.map_err(|e| format!("launch count: {e:?}"))?;
        }
        let cnt = stream
            .memcpy_dtov(&d_cnt)
            .map_err(|e| format!("dtoh cnt: {e:?}"))?;

        // Host exclusive scan → offsets (length n+1); off[n] is the total pair count.
        let mut off = Vec::with_capacity(n + 1);
        let mut acc: i64 = 0;
        for &c in &cnt {
            off.push(acc as i32);
            acc += c as i64;
        }
        off.push(acc as i32);
        let total = acc as usize;
        if total == 0 {
            return Ok(Vec::new());
        }

        let lo = stream
            .memcpy_dtov(&d_lo)
            .map_err(|e| format!("dtoh lo: {e:?}"))?;
        let d_left_x = stream
            .memcpy_stod(&left_x)
            .map_err(|e| format!("htod left_x: {e:?}"))?;
        let d_right_val = stream
            .memcpy_stod(&right_val)
            .map_err(|e| format!("htod right_val: {e:?}"))?;
        let d_lo2 = stream
            .memcpy_stod(&lo)
            .map_err(|e| format!("htod lo: {e:?}"))?;
        let d_off = stream
            .memcpy_stod(&off)
            .map_err(|e| format!("htod off: {e:?}"))?;
        let mut d_out_x = stream
            .alloc_zeros::<i32>(total)
            .map_err(|e| format!("alloc out_x: {e:?}"))?;
        let mut d_out_z = stream
            .alloc_zeros::<i32>(total)
            .map_err(|e| format!("alloc out_z: {e:?}"))?;
        {
            let mut b = stream.launch_builder(&self.scatter);
            b.arg(&d_left_x)
                .arg(&n_i32)
                .arg(&d_right_val)
                .arg(&d_lo2)
                .arg(&d_off)
                .arg(&mut d_out_x)
                .arg(&mut d_out_z);
            unsafe { b.launch(cfg) }.map_err(|e| format!("launch scatter: {e:?}"))?;
        }
        let out_x = stream
            .memcpy_dtov(&d_out_x)
            .map_err(|e| format!("dtoh out_x: {e:?}"))?;
        let out_z = stream
            .memcpy_dtov(&d_out_z)
            .map_err(|e| format!("dtoh out_z: {e:?}"))?;
        Ok(out_x
            .into_iter()
            .zip(out_z)
            .map(|(x, z)| (x as u32, z as u32))
            .collect())
    }
}

impl ClosureBackend for CudaBackend {
    fn name(&self) -> &'static str {
        "cuda"
    }

    fn join_on_middle(&self, left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)> {
        match self.try_join(left, right) {
            Ok(out) => out,
            Err(e) => {
                tracing::warn!("cuda transitive-join failed ({e}); CPU fallback");
                CpuBackend.join_on_middle(left, right)
            }
        }
    }
}

/// The process-global CUDA backend, `Some` only if a device initialised
/// (CONCEPT:EG-KG.backend.real-cuda-tensor-backend). `EPISTEMIC_GRAPH_ANN_DEVICE`
/// selects the ordinal (default 0, shared with the ANN/distance backends). Cached, so
/// device init + kernel compile happen once.
pub fn backend() -> Option<&'static dyn ClosureBackend> {
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
                tracing::info!("CUDA transitive-join backend unavailable ({e}); using CPU");
                None
            }
            Err(_) => {
                tracing::info!("CUDA driver not loadable; using CPU transitive-join backend");
                None
            }
        }
    })
    .as_ref()
    .map(|b| b as &dyn ClosureBackend)
}
