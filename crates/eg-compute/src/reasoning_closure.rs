//! CONCEPT:EG-KG.compute.reasoning-closure-gpu — semi-naive Datalog closure evaluator
//! with a GPU-offloadable transitive-join seam.
//!
//! This is the re-architecture the S4 deferral note in `reasoning.rs` called for. The
//! original five-rule fixpoint was a HETEROGENEOUS naive evaluation over string-keyed
//! `HashMap`/`HashSet` structures — re-scanning the ENTIRE accumulated fact set on every
//! iteration — with only Rule 5 (transitive closure) shaped like a sparse-matrix step.
//! Two changes fix that:
//!
//!   1. **Integer interning + semi-naive evaluation.** Node ids and type/predicate
//!      labels are interned to `u32` once ([`Interner`]); the fixpoint then works over
//!      integer relations and derives new facts from the per-round DELTA only (a fact can
//!      only be new if one of its premises was new last round), not by re-scanning the
//!      whole relation every iteration. Same result set, far less repeated work.
//!   2. **Rule 5 behind a [`ClosureBackend`] seam.** The transitive step is a boolean
//!      semiring join `{(x,z) | (x,y)∈A, (y,z)∈B}`. It is factored behind a trait with an
//!      always-compiled [`CpuBackend`] (hash-join) and a feature-gated
//!      `cuda::CudaBackend` (a two-pass CSR join kernel), mirroring
//!      `eg-ann::kmeans_gpu`'s `AssignBackend` seam EXACTLY — CPU-only builds link no
//!      accelerator, and any device/compile/launch failure degrades that call to CPU.
//!
//! Correctness anchors (see `tests`): [`infer_semi_naive`] derives the SAME set of facts
//! as the prior naive fixpoint ([`infer_naive_reference`], the differential oracle), and
//! the CUDA transitive-join agrees pair-for-pair with the CPU join (the parity test SKIPs
//! cleanly on a GPU-less host and auto-validates on a real device such as the GB10).
//!
//! Pi contract: identical to `eg-ann` — the CPU path needs no feature; `gpu`/`gpu-cuda`
//! are OUT of `pi`/`default`/`full` and `cudarc` links only under `gpu-cuda` (dlopen at
//! runtime, so a `gpu-cuda` build compiles with no CUDA toolkit and runs on a GPU-less
//! host).

use std::collections::{HashMap, HashSet};

/// String ↔ `u32` interner shared by node ids and type/predicate labels. One flat table:
/// a string that is both a node id and a label maps to a single id (they live in
/// different relations, so no ambiguity), which keeps interning a single pass.
#[derive(Default)]
pub struct Interner {
    to_id: HashMap<String, u32>,
    from_id: Vec<String>,
}

impl Interner {
    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.to_id.get(s) {
            return id;
        }
        let id = self.from_id.len() as u32;
        self.from_id.push(s.to_string());
        self.to_id.insert(s.to_string(), id);
        id
    }

    fn resolve(&self, id: u32) -> &str {
        &self.from_id[id as usize]
    }
}

/// The transitive-closure join backend (CONCEPT:EG-KG.compute.reasoning-closure-gpu):
/// computes the boolean-semiring join `{(x,z) | (x,y)∈left, (y,z)∈right}`. The returned
/// pairs may contain duplicates and pairs already known to the caller — the semi-naive
/// driver dedups against the accumulated relation. Every backend MUST return the SAME set
/// of pairs so a GPU-built closure is interchangeable with the CPU one.
pub trait ClosureBackend: Send + Sync {
    /// Stable backend name for logs/tests (`"cpu"`, `"cuda"`).
    fn name(&self) -> &'static str;

    /// `{(x, z) | (x, y) ∈ left, (y, z) ∈ right}` — join on the shared middle key.
    fn join_on_middle(&self, left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)>;
}

/// The always-compiled pure-Rust CPU backend (CONCEPT:EG-KG.compute.reasoning-closure-gpu).
/// Hash-joins on the middle key: index `right` by its source, then for every `left`
/// `(x, y)` emit `(x, z)` for each `z` in `right[y]`.
#[derive(Debug, Default, Clone, Copy)]
pub struct CpuBackend;

impl ClosureBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn join_on_middle(&self, left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)> {
        if left.is_empty() || right.is_empty() {
            return Vec::new();
        }
        let mut by_src: HashMap<u32, Vec<u32>> = HashMap::new();
        for &(y, z) in right {
            by_src.entry(y).or_default().push(z);
        }
        let mut out = Vec::new();
        for &(x, y) in left {
            if let Some(zs) = by_src.get(&y) {
                for &z in zs {
                    out.push((x, z));
                }
            }
        }
        out
    }
}

/// The active transitive-join backend (CONCEPT:EG-KG.compute.reasoning-closure-gpu): CUDA
/// when compiled + a device is present, else CPU. Built once and cached.
pub fn active_closure_backend() -> &'static dyn ClosureBackend {
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
pub fn active_closure_backend_name() -> &'static str {
    active_closure_backend().name()
}

/// Rule inputs, interned to `u32`. Built once by [`infer_semi_naive`] from the string
/// rule lists so the fixpoint never touches strings.
struct Rules {
    subclass: HashMap<u32, Vec<u32>>,
    subprop: HashMap<u32, Vec<u32>>,
    symmetric: HashSet<u32>,
    transitive: Vec<u32>,
    inverse: HashMap<u32, u32>,
}

/// Derive all facts entailed by the five OWL/RDFS rules (subclass, subproperty,
/// symmetric, inverse, transitive) via SEMI-NAIVE evaluation over interned integer
/// relations, offloading the transitive join to `backend`.
///
/// Inputs are the base facts and the rule lists as strings; returns the DERIVED facts
/// (accumulated minus base) as strings:
///   * `.0` — new `(node, type)` facts (Rule 1),
///   * `.1` — new `(src, tgt, prop)` edge facts (Rules 2–5).
///
/// The result set is identical to the prior naive fixpoint (see the differential test).
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn infer_semi_naive(
    base_node_types: &[(String, String)],
    base_edge_types: &[(String, String, String)],
    subclass_relations: Vec<(String, String)>,
    subproperty_relations: Vec<(String, String)>,
    symmetric_properties: Vec<String>,
    transitive_properties: Vec<String>,
    inverse_properties: Vec<(String, String)>,
    backend: &dyn ClosureBackend,
) -> (Vec<(String, String)>, Vec<(String, String, String)>) {
    let mut it = Interner::default();

    // Intern the rule lists.
    let mut subclass: HashMap<u32, Vec<u32>> = HashMap::new();
    for (sub, sup) in subclass_relations {
        subclass
            .entry(it.intern(&sub))
            .or_default()
            .push(it.intern(&sup));
    }
    let mut subprop: HashMap<u32, Vec<u32>> = HashMap::new();
    for (sub, sup) in subproperty_relations {
        subprop
            .entry(it.intern(&sub))
            .or_default()
            .push(it.intern(&sup));
    }
    let symmetric: HashSet<u32> = symmetric_properties.iter().map(|p| it.intern(p)).collect();
    let mut inverse: HashMap<u32, u32> = HashMap::new();
    for (p1, p2) in inverse_properties {
        let a = it.intern(&p1);
        let b = it.intern(&p2);
        inverse.insert(a, b);
        inverse.insert(b, a);
    }
    let transitive: Vec<u32> = transitive_properties.iter().map(|p| it.intern(p)).collect();
    let rules = Rules {
        subclass,
        subprop,
        symmetric,
        transitive,
        inverse,
    };

    // Intern base facts into integer relations.
    let mut nt_set: HashSet<(u32, u32)> = HashSet::new();
    let mut delta_nt: Vec<(u32, u32)> = Vec::new();
    for (n, t) in base_node_types {
        let f = (it.intern(n), it.intern(t));
        if nt_set.insert(f) {
            delta_nt.push(f);
        }
    }
    let mut et_set: HashSet<(u32, u32, u32)> = HashSet::new();
    let mut delta_et: Vec<(u32, u32, u32)> = Vec::new();
    // Accumulated transitive edges grouped by predicate (only predicates that are
    // transitive need grouping — that is all Rule 5 consults).
    let mut et_by_prop: HashMap<u32, Vec<(u32, u32)>> = HashMap::new();
    let transitive_set: HashSet<u32> = rules.transitive.iter().copied().collect();
    for (s, g, p) in base_edge_types {
        let f = (it.intern(s), it.intern(g), it.intern(p));
        if et_set.insert(f) {
            delta_et.push(f);
            if transitive_set.contains(&f.2) {
                et_by_prop.entry(f.2).or_default().push((f.0, f.1));
            }
        }
    }

    let mut derived_nt: Vec<(u32, u32)> = Vec::new();
    let mut derived_et: Vec<(u32, u32, u32)> = Vec::new();

    // Semi-naive fixpoint: each round derives only from the previous round's delta. The
    // 100-round cap mirrors the naive evaluator's safety bound (these monotone rules
    // converge in a handful of rounds on real ontologies).
    let mut round = 0;
    while (!delta_nt.is_empty() || !delta_et.is_empty()) && round < 100 {
        round += 1;
        let mut cand_nt: Vec<(u32, u32)> = Vec::new();
        let mut cand_et: Vec<(u32, u32, u32)> = Vec::new();

        // Rule 1 (subclass): a node with a type gains its supertypes. Semi-naive over
        // the new node-type facts only.
        for &(node, t) in &delta_nt {
            if let Some(sups) = rules.subclass.get(&t) {
                for &sup in sups {
                    cand_nt.push((node, sup));
                }
            }
        }

        // Rules 2–4 over the new edge facts only.
        for &(s, g, p) in &delta_et {
            // Rule 2 (subproperty): the edge gains its super-properties.
            if let Some(sups) = rules.subprop.get(&p) {
                for &sup in sups {
                    cand_et.push((s, g, sup));
                }
            }
            // Rule 3 (symmetric): the reverse edge gains the same property.
            if rules.symmetric.contains(&p) {
                cand_et.push((g, s, p));
            }
            // Rule 4 (inverse): the reverse edge gains the inverse property.
            if let Some(&inv) = rules.inverse.get(&p) {
                cand_et.push((g, s, inv));
            }
        }

        // Rule 5 (transitive): boolean-semiring join for each transitive predicate,
        // offloaded to `backend`. Semi-naive form: new 2-paths use at least one delta
        // edge, so join both DELTA⋈FULL and FULL⋈DELTA (FULL already contains DELTA, so
        // this also covers DELTA⋈DELTA). This is the one sparse-matrix-shaped rule and
        // the sole GPU seam.
        if !rules.transitive.is_empty() {
            // This round's delta edges grouped by (transitive) predicate.
            let mut delta_by_prop: HashMap<u32, Vec<(u32, u32)>> = HashMap::new();
            for &(s, g, p) in &delta_et {
                if transitive_set.contains(&p) {
                    delta_by_prop.entry(p).or_default().push((s, g));
                }
            }
            for (&p, dp) in &delta_by_prop {
                let full = et_by_prop.get(&p).map(Vec::as_slice).unwrap_or(&[]);
                for (x, z) in backend.join_on_middle(dp, full) {
                    cand_et.push((x, z, p));
                }
                for (x, z) in backend.join_on_middle(full, dp) {
                    cand_et.push((x, z, p));
                }
            }
        }

        // Accept the candidates not already known; they become the next round's delta.
        let mut next_nt: Vec<(u32, u32)> = Vec::new();
        for f in cand_nt {
            if nt_set.insert(f) {
                next_nt.push(f);
                derived_nt.push(f);
            }
        }
        let mut next_et: Vec<(u32, u32, u32)> = Vec::new();
        for f in cand_et {
            if et_set.insert(f) {
                next_et.push(f);
                derived_et.push(f);
                if transitive_set.contains(&f.2) {
                    et_by_prop.entry(f.2).or_default().push((f.0, f.1));
                }
            }
        }
        delta_nt = next_nt;
        delta_et = next_et;
    }

    // Resolve derived facts back to strings.
    let out_nt = derived_nt
        .into_iter()
        .map(|(n, t)| (it.resolve(n).to_string(), it.resolve(t).to_string()))
        .collect();
    let out_et = derived_et
        .into_iter()
        .map(|(s, g, p)| {
            (
                it.resolve(s).to_string(),
                it.resolve(g).to_string(),
                it.resolve(p).to_string(),
            )
        })
        .collect();
    (out_nt, out_et)
}

// ── CONCEPT:EG-KG.backend.real-cuda-tensor-backend — the real CUDA transitive-join backend ──

#[cfg(feature = "gpu-cuda")]
pub mod cuda {
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
            let ptx =
                cudarc::nvrtc::compile_ptx(KERNEL_SRC).map_err(|e| format!("nvrtc: {e:?}"))?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    /// NAIVE reference evaluator — the pre-existing string-keyed five-rule fixpoint,
    /// distilled to pure inference over the base fact lists (no graph mutation). This is
    /// the differential ORACLE: [`infer_semi_naive`] must return the same DERIVED sets.
    #[allow(clippy::type_complexity)]
    fn infer_naive_reference(
        base_node_types: &[(String, String)],
        base_edge_types: &[(String, String, String)],
        subclass_relations: &[(String, String)],
        subproperty_relations: &[(String, String)],
        symmetric_properties: &[String],
        transitive_properties: &[String],
        inverse_properties: &[(String, String)],
    ) -> (HashSet<(String, String)>, HashSet<(String, String, String)>) {
        let mut subclass_map: HashMap<String, Vec<String>> = HashMap::new();
        for (sub, sup) in subclass_relations {
            subclass_map
                .entry(sub.clone())
                .or_default()
                .push(sup.clone());
        }
        let mut subprop_map: HashMap<String, Vec<String>> = HashMap::new();
        for (sub, sup) in subproperty_relations {
            subprop_map
                .entry(sub.clone())
                .or_default()
                .push(sup.clone());
        }
        let symmetric_set: HashSet<String> = symmetric_properties.iter().cloned().collect();
        let transitive_set: HashSet<String> = transitive_properties.iter().cloned().collect();
        let mut inverse_map: HashMap<String, String> = HashMap::new();
        for (p1, p2) in inverse_properties {
            inverse_map.insert(p1.clone(), p2.clone());
            inverse_map.insert(p2.clone(), p1.clone());
        }

        let mut node_types: HashMap<String, HashSet<String>> = HashMap::new();
        for (n, t) in base_node_types {
            node_types.entry(n.clone()).or_default().insert(t.clone());
        }
        let mut edge_types: HashMap<(String, String), HashSet<String>> = HashMap::new();
        for (s, g, p) in base_edge_types {
            edge_types
                .entry((s.clone(), g.clone()))
                .or_default()
                .insert(p.clone());
        }

        let mut new_nt: HashSet<(String, String)> = HashSet::new();
        let mut new_et: HashSet<(String, String, String)> = HashSet::new();
        let mut changed = true;
        let mut iters = 0;
        while changed && iters < 100 {
            changed = false;
            let mut pend_nt = Vec::new();
            let mut pend_et = Vec::new();
            for (node, types) in &node_types {
                for t in types {
                    if let Some(sups) = subclass_map.get(t) {
                        for sup in sups {
                            if !types.contains(sup) {
                                pend_nt.push((node.clone(), sup.clone()));
                            }
                        }
                    }
                }
            }
            for ((s, g), types) in &edge_types {
                for t in types {
                    if let Some(sups) = subprop_map.get(t) {
                        for sup in sups {
                            if !types.contains(sup) {
                                pend_et.push((s.clone(), g.clone(), sup.clone()));
                            }
                        }
                    }
                    if symmetric_set.contains(t) {
                        let exists = edge_types
                            .get(&(g.clone(), s.clone()))
                            .is_some_and(|ts| ts.contains(t));
                        if !exists {
                            pend_et.push((g.clone(), s.clone(), t.clone()));
                        }
                    }
                    if let Some(inv) = inverse_map.get(t) {
                        let exists = edge_types
                            .get(&(g.clone(), s.clone()))
                            .is_some_and(|ts| ts.contains(inv));
                        if !exists {
                            pend_et.push((g.clone(), s.clone(), inv.clone()));
                        }
                    }
                }
            }
            for p in &transitive_set {
                let mut p_edges = Vec::new();
                for ((s, g), ts) in &edge_types {
                    if ts.contains(p) {
                        p_edges.push((s.clone(), g.clone()));
                    }
                }
                for (x, y) in &p_edges {
                    for (y2, z) in &p_edges {
                        if y == y2 {
                            let exists = edge_types
                                .get(&(x.clone(), z.clone()))
                                .is_some_and(|ts| ts.contains(p));
                            if !exists {
                                pend_et.push((x.clone(), z.clone(), p.clone()));
                            }
                        }
                    }
                }
            }
            for (node, t) in pend_nt {
                if node_types
                    .entry(node.clone())
                    .or_default()
                    .insert(t.clone())
                {
                    new_nt.insert((node, t));
                    changed = true;
                }
            }
            for (s, g, p) in pend_et {
                if edge_types
                    .entry((s.clone(), g.clone()))
                    .or_default()
                    .insert(p.clone())
                {
                    new_et.insert((s, g, p));
                    changed = true;
                }
            }
            iters += 1;
        }
        (new_nt, new_et)
    }

    #[test]
    fn cpu_join_on_middle_matches_definition() {
        let left = vec![(1u32, 2u32), (1, 3), (4, 2)];
        let right = vec![(2u32, 9u32), (2, 8), (3, 7)];
        let got: HashSet<(u32, u32)> = CpuBackend
            .join_on_middle(&left, &right)
            .into_iter()
            .collect();
        let want: HashSet<(u32, u32)> = [(1, 9), (1, 8), (1, 7), (4, 9), (4, 8)]
            .into_iter()
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn semi_naive_transitive_closure() {
        let edges = vec![
            ("a".into(), "b".into(), "anc".into()),
            ("b".into(), "c".into(), "anc".into()),
            ("c".into(), "d".into(), "anc".into()),
        ];
        let (_nt, et) = infer_semi_naive(
            &[],
            &edges,
            vec![],
            vec![],
            vec![],
            vec!["anc".into()],
            vec![],
            &CpuBackend,
        );
        let set: HashSet<(String, String, String)> = et.into_iter().collect();
        // a→c, a→d, b→d are the transitive closures (a→b, b→c, c→d are base).
        assert!(set.contains(&("a".into(), "c".into(), "anc".into())));
        assert!(set.contains(&("a".into(), "d".into(), "anc".into())));
        assert!(set.contains(&("b".into(), "d".into(), "anc".into())));
        assert_eq!(set.len(), 3);
    }

    /// DIFFERENTIAL ORACLE: over randomized ontologies exercising all five rules, the
    /// semi-naive evaluator derives EXACTLY the naive reference's fact set.
    #[test]
    fn semi_naive_equals_naive_reference_randomized() {
        for seed in 0..40u64 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let n_nodes = rng.gen_range(3..10);
            let types = ["T0", "T1", "T2", "T3"];
            let props = ["p0", "p1", "p2"];

            let mut base_nt = Vec::new();
            for i in 0..n_nodes {
                if rng.gen_bool(0.7) {
                    base_nt.push((format!("n{i}"), types[rng.gen_range(0..types.len())].into()));
                }
            }
            let mut base_et = Vec::new();
            let n_edges = rng.gen_range(2..12);
            for _ in 0..n_edges {
                let s = format!("n{}", rng.gen_range(0..n_nodes));
                let g = format!("n{}", rng.gen_range(0..n_nodes));
                base_et.push((s, g, props[rng.gen_range(0..props.len())].into()));
            }
            // Rule config, some chains to force multi-round fixpoints.
            let subclass = vec![
                ("T0".to_string(), "T1".to_string()),
                ("T1".to_string(), "T2".to_string()),
            ];
            let subprop = vec![("p0".to_string(), "p1".to_string())];
            let symmetric = if rng.gen_bool(0.5) {
                vec!["p2".to_string()]
            } else {
                vec![]
            };
            let transitive = vec!["p1".to_string()];
            let inverse = vec![("p0".to_string(), "p2".to_string())];

            let (naive_nt, naive_et) = infer_naive_reference(
                &base_nt,
                &base_et,
                &subclass,
                &subprop,
                &symmetric,
                &transitive,
                &inverse,
            );
            let (sn_nt_v, sn_et_v) = infer_semi_naive(
                &base_nt,
                &base_et,
                subclass,
                subprop,
                symmetric,
                transitive,
                inverse,
                &CpuBackend,
            );
            let sn_nt: HashSet<(String, String)> = sn_nt_v.into_iter().collect();
            let sn_et: HashSet<(String, String, String)> = sn_et_v.into_iter().collect();
            assert_eq!(sn_nt, naive_nt, "node-type set mismatch (seed {seed})");
            assert_eq!(sn_et, naive_et, "edge set mismatch (seed {seed})");
        }
    }

    #[test]
    fn dispatch_backend_is_named() {
        let name = active_closure_backend_name();
        assert!(name == "cpu" || name == "cuda");
    }

    /// GPU↔CPU parity (CONCEPT:EG-KG.compute.reasoning-closure-gpu). When a CUDA device is
    /// present the real transitive-join kernel MUST produce the SAME pair SET as the CPU
    /// hash-join for a batch spanning several thread blocks; when no device is available
    /// `cuda::backend()` is `None` and the test SKIPs cleanly. So it is a no-op in GPU-less
    /// CI yet auto-validates the kernel wherever a GPU exists (e.g. the GB10). Only
    /// compiled under `--features gpu-cuda`.
    #[cfg(feature = "gpu-cuda")]
    #[test]
    fn cuda_join_matches_cpu_ground_truth() {
        let Some(gpu) = cuda::backend() else {
            eprintln!("SKIP cuda_join_matches_cpu: no CUDA device present (CPU-only host)");
            return;
        };
        assert_eq!(gpu.name(), "cuda", "backend() returned a non-CUDA backend");

        let mut rng = ChaCha8Rng::seed_from_u64(9);
        let keyspace = 128u32;
        let left: Vec<(u32, u32)> = (0..5000)
            .map(|_| (rng.gen_range(0..keyspace), rng.gen_range(0..keyspace)))
            .collect();
        let right: Vec<(u32, u32)> = (0..5000)
            .map(|_| (rng.gen_range(0..keyspace), rng.gen_range(0..keyspace)))
            .collect();

        let cpu: HashSet<(u32, u32)> = CpuBackend
            .join_on_middle(&left, &right)
            .into_iter()
            .collect();
        let gpu_set: HashSet<(u32, u32)> = gpu.join_on_middle(&left, &right).into_iter().collect();
        assert_eq!(cpu, gpu_set, "GPU transitive-join set != CPU ground truth");
    }
}
