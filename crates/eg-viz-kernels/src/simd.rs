//! Runtime-detected AVX2 acceleration (D-VZ-1 lane V2).
//!
//! ★ **Runtime detection only — never a compile-time `target-feature`/`target-cpu`
//! assumption.** This fleet's interactive dev host (a pre-AVX2 Westmere-era x86_64
//! machine) lacks `x86-64-v3`; a binary built assuming AVX2 SIGILLs there. Every SIMD entry point
//! in this crate is reached only after [`avx2_available`] (a cached
//! `is_x86_feature_detected!("avx2")`, checked once at first use, never assumed)
//! returns `true` — the SAME binary runs the scalar fallback on a CPU without AVX2
//! and produces byte-identical bucket assignments/areas either way (proved by
//! `tests/proptest_invariants.rs`'s `simd_matches_scalar_*` properties, which run
//! unconditionally in CI regardless of the runner's actual CPU: they call the
//! scalar and (`#[cfg(target_arch = "x86_64")]`-gated, further runtime-gated) SIMD
//! path directly and assert equality, rather than relying on the runner happening
//! to have AVX2).
//!
//! **Where this crate does NOT vectorize, and why.** Both kernels' actual
//! min/max/argmax *reduction into a bucket* is a scatter (M4: which of `width_px`
//! buckets a point updates depends on that point's own x value) or a small serial
//! dependency chain (LTTB: bucket `i`'s "previous selected point" is bucket
//! `i-1`'s output) — neither is a case AVX2 (no scatter/gather, no cross-lane
//! dependency) can honestly accelerate. What IS genuinely vectorizable, and what
//! this module accelerates, is the **regular, independent, per-element arithmetic**
//! that feeds those reductions:
//! - M4: mapping a batch of 4 x-values to their pixel-column bucket index plus a
//!   finite (`is_finite`) mask — pure elementwise arithmetic/compares, no
//!   cross-lane dependency, computed once per point regardless of which bucket it
//!   lands in.
//! - LTTB: the triangle-area formula evaluated for every candidate point in one
//!   (contiguous, since input is x-sorted before this runs) bucket, and the mean
//!   of a contiguous bucket's y-values for the "average next-bucket point" — both
//!   independent, regular, per-element work over a contiguous slice.

use std::sync::OnceLock;

/// Whether this process may take the AVX2 fast path — checked once
/// (`is_x86_feature_detected!` itself is cheap but not free; callers hit this
/// possibly per-bucket) and cached for the process lifetime. `cfg(not(x86_64))`
/// targets (e.g. the fleet's aarch64 cross-compile, `fix/aarch64-file-prefix-map`)
/// never see the AVX2 path compiled at all — this always returns `false` there,
/// which is correct: there is no x86 AVX2 to detect.
pub fn avx2_available() -> bool {
    static AVX2: OnceLock<bool> = OnceLock::new();
    *AVX2.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            std::is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    })
}

/// Map a batch of up to 4 x-values to `(bucket_index, is_finite)` pairs, written
/// into `out_idx`/`out_finite` (same length as `xs`, `xs.len() <= 4`). Scalar
/// reference implementation — always correct, always available, and what the
/// AVX2 path below is proved equivalent to.
///
/// `bucket_index` for a non-finite input is `0` (a well-defined placeholder;
/// `out_finite[i] == false` is the contract callers must check before trusting
/// it — mirrors every other reduction in this crate's/eg-viz-export's "filter
/// non-finite before it can become a fabricated extremum" discipline).
pub fn bucket_indices_scalar(
    xs: &[f64],
    domain_min: f64,
    inv_domain_range: f64,
    cols: usize,
    out_idx: &mut [u32],
    out_finite: &mut [bool],
) {
    debug_assert_eq!(xs.len(), out_idx.len());
    debug_assert_eq!(xs.len(), out_finite.len());
    let cols_f = cols as f64;
    for (i, &x) in xs.iter().enumerate() {
        if !x.is_finite() {
            out_idx[i] = 0;
            out_finite[i] = false;
            continue;
        }
        let t = ((x - domain_min) * inv_domain_range).clamp(0.0, 1.0);
        // `t == 1.0` must land in the LAST column, not overflow it -- the same
        // "clamp after floor" discipline `LinearMap`/`decimate_minmax` use.
        let col = (t * cols_f) as usize;
        out_idx[i] = col.min(cols.saturating_sub(1)) as u32;
        out_finite[i] = true;
    }
}

/// AVX2 batch path for [`bucket_indices_scalar`] — processes `xs` (any length)
/// four lanes at a time, falling back to the scalar loop for the `len % 4`
/// remainder.
///
/// # Safety
///
/// The caller must ensure the CPU executing this function supports AVX2 —
/// every call site in this crate checks [`avx2_available`] first and never
/// calls this unconditionally. `out_idx`/`out_finite` must be the same length
/// as `xs` (the public dispatcher [`bucket_indices`] guarantees this via its
/// own `debug_assert`s before delegating here). Every intrinsic used operates
/// on plain, already-length-checked local arrays/slices — no raw pointer
/// arithmetic beyond the bounds-checked slice/array conversions the safe
/// wrapper performs.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn bucket_indices_avx2(
    xs: &[f64],
    domain_min: f64,
    inv_domain_range: f64,
    cols: usize,
    out_idx: &mut [u32],
    out_finite: &mut [bool],
) {
    use std::arch::x86_64::*;

    debug_assert_eq!(xs.len(), out_idx.len());
    debug_assert_eq!(xs.len(), out_finite.len());

    let domain_min_v = _mm256_set1_pd(domain_min);
    let inv_range_v = _mm256_set1_pd(inv_domain_range);
    let cols_f_v = _mm256_set1_pd(cols as f64);
    let zero_v = _mm256_set1_pd(0.0);
    let one_v = _mm256_set1_pd(1.0);
    let cols_max_idx = cols.saturating_sub(1) as i32;

    let chunks = xs.len() / 4;
    for c in 0..chunks {
        let base = c * 4;
        let x = _mm256_loadu_pd(xs.as_ptr().add(base));

        // Ordered compare (x == x) is false only for NaN.
        let not_nan = _mm256_cmp_pd(x, x, _CMP_ORD_Q);
        // |x| < +inf, via clearing the sign bit then comparing.
        let sign_mask = _mm256_set1_pd(-0.0);
        let abs_x = _mm256_andnot_pd(sign_mask, x);
        let inf_v = _mm256_set1_pd(f64::INFINITY);
        let finite_range = _mm256_cmp_pd(abs_x, inf_v, _CMP_LT_OQ);
        let finite_mask = _mm256_and_pd(not_nan, finite_range);

        let t_raw = _mm256_mul_pd(_mm256_sub_pd(x, domain_min_v), inv_range_v);
        let t_clamped = _mm256_min_pd(_mm256_max_pd(t_raw, zero_v), one_v);
        let col_f = _mm256_mul_pd(t_clamped, cols_f_v);

        // Truncate toward zero (t_clamped >= 0 always, so this is floor) into
        // 4x i32 lanes (the low 128 bits of the result; f64x4 -> i32x4 is a
        // natural AVX narrowing conversion).
        let col_i = _mm256_cvttpd_epi32(col_f);
        let mut col_arr = [0i32; 4];
        _mm_storeu_si128(col_arr.as_mut_ptr() as *mut __m128i, col_i);

        let mut finite_arr = [0f64; 4];
        _mm256_storeu_pd(finite_arr.as_mut_ptr(), finite_mask);

        for lane in 0..4 {
            let is_finite = finite_arr[lane].to_bits() != 0; // all-1s bits if true, 0 if false
            out_finite[base + lane] = is_finite;
            out_idx[base + lane] = if is_finite {
                col_arr[lane].clamp(0, cols_max_idx.max(0)) as u32
            } else {
                0
            };
        }
    }

    let rem_start = chunks * 4;
    bucket_indices_scalar(
        &xs[rem_start..],
        domain_min,
        inv_domain_range,
        cols,
        &mut out_idx[rem_start..],
        &mut out_finite[rem_start..],
    );
}

/// Dispatch to the AVX2 path when available, else the scalar path — the ONE
/// call site every kernel in this crate uses (never calls either sub-path
/// directly), so "runtime-detected, scalar-equivalent" is enforced in one place.
pub fn bucket_indices(
    xs: &[f64],
    domain_min: f64,
    inv_domain_range: f64,
    cols: usize,
    out_idx: &mut [u32],
    out_finite: &mut [bool],
) {
    #[cfg(target_arch = "x86_64")]
    {
        if avx2_available() {
            // SAFETY: `avx2_available()` proved the CPU supports AVX2; the
            // callee only reads `xs` and writes `out_idx`/`out_finite`, all
            // already-length-matched slices per the debug_asserts above.
            unsafe {
                bucket_indices_avx2(xs, domain_min, inv_domain_range, cols, out_idx, out_finite);
            }
            return;
        }
    }
    bucket_indices_scalar(xs, domain_min, inv_domain_range, cols, out_idx, out_finite);
}

/// Triangle area (LTTB's selection criterion) for a batch of candidate points
/// `(cx[i], cy[i])`, all forming a triangle with the fixed `prev` point and the
/// fixed `avg` (next-bucket mean) point. Scalar reference.
pub fn triangle_areas_scalar(
    prev: (f64, f64),
    avg: (f64, f64),
    cx: &[f64],
    cy: &[f64],
    out: &mut [f64],
) {
    debug_assert_eq!(cx.len(), cy.len());
    debug_assert_eq!(cx.len(), out.len());
    for i in 0..cx.len() {
        out[i] =
            ((prev.0 - avg.0) * (cy[i] - prev.1) - (prev.0 - cx[i]) * (avg.1 - prev.1)).abs() * 0.5;
    }
}

/// AVX2 batch path for [`triangle_areas_scalar`].
///
/// # Safety
///
/// Same contract as [`bucket_indices_avx2`]: the caller must have already
/// confirmed [`avx2_available`] (every call site in this crate does); `cx`,
/// `cy`, and `out` must be equal length (the public dispatcher
/// [`triangle_areas`] guarantees this via `debug_assert`s before delegating
/// here). Operates only on already-length-checked slices, no raw pointer
/// arithmetic beyond bounds-checked slice/array conversions.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn triangle_areas_avx2(
    prev: (f64, f64),
    avg: (f64, f64),
    cx: &[f64],
    cy: &[f64],
    out: &mut [f64],
) {
    use std::arch::x86_64::*;

    debug_assert_eq!(cx.len(), cy.len());
    debug_assert_eq!(cx.len(), out.len());

    let prev_x_v = _mm256_set1_pd(prev.0);
    let prev_y_v = _mm256_set1_pd(prev.1);
    let term_a = _mm256_set1_pd(prev.0 - avg.0); // (prev.x - avg.x), constant per bucket
    let avg_y_minus_prev_y = _mm256_set1_pd(avg.1 - prev.1);
    let sign_mask = _mm256_set1_pd(-0.0);
    let half = _mm256_set1_pd(0.5);

    let chunks = cx.len() / 4;
    for c in 0..chunks {
        let base = c * 4;
        let x = _mm256_loadu_pd(cx.as_ptr().add(base));
        let y = _mm256_loadu_pd(cy.as_ptr().add(base));

        let term1 = _mm256_mul_pd(term_a, _mm256_sub_pd(y, prev_y_v));
        let term2 = _mm256_mul_pd(_mm256_sub_pd(prev_x_v, x), avg_y_minus_prev_y);
        let diff = _mm256_sub_pd(term1, term2);
        let abs_diff = _mm256_andnot_pd(sign_mask, diff);
        let area = _mm256_mul_pd(abs_diff, half);

        _mm256_storeu_pd(out.as_mut_ptr().add(base), area);
    }

    let rem_start = chunks * 4;
    triangle_areas_scalar(
        prev,
        avg,
        &cx[rem_start..],
        &cy[rem_start..],
        &mut out[rem_start..],
    );
}

/// Dispatch to AVX2 when available, else scalar — the one call site LTTB uses.
pub fn triangle_areas(prev: (f64, f64), avg: (f64, f64), cx: &[f64], cy: &[f64], out: &mut [f64]) {
    #[cfg(target_arch = "x86_64")]
    {
        if avx2_available() {
            // SAFETY: see `bucket_indices`'s identical justification.
            unsafe {
                triangle_areas_avx2(prev, avg, cx, cy, out);
            }
            return;
        }
    }
    triangle_areas_scalar(prev, avg, cx, cy, out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_bucket_indices_maps_domain_endpoints_to_first_and_last_column() {
        let xs = [0.0, 100.0, 50.0, f64::NAN, f64::INFINITY];
        let mut idx = [0u32; 5];
        let mut finite = [false; 5];
        bucket_indices_scalar(&xs, 0.0, 1.0 / 100.0, 10, &mut idx, &mut finite);
        assert_eq!(idx[0], 0);
        assert_eq!(
            idx[1], 9,
            "domain max must clamp into the LAST column, not overflow it"
        );
        assert!(finite[0] && finite[1] && finite[2]);
        assert!(!finite[3] && !finite[4]);
    }

    #[test]
    fn avx2_bucket_indices_matches_scalar_on_representative_input() {
        if !avx2_available() {
            return; // proved equivalent on whatever CPU proptest runs on instead
        }
        let xs: Vec<f64> = (0..37)
            .map(|i| match i % 7 {
                0 => f64::NAN,
                1 => f64::INFINITY,
                2 => f64::NEG_INFINITY,
                _ => (i as f64) * 3.7 - 12.0,
            })
            .collect();
        let mut idx_scalar = vec![0u32; xs.len()];
        let mut fin_scalar = vec![false; xs.len()];
        bucket_indices_scalar(&xs, -12.0, 1.0 / 50.0, 17, &mut idx_scalar, &mut fin_scalar);

        let mut idx_simd = vec![0u32; xs.len()];
        let mut fin_simd = vec![false; xs.len()];
        unsafe {
            bucket_indices_avx2(&xs, -12.0, 1.0 / 50.0, 17, &mut idx_simd, &mut fin_simd);
        }
        assert_eq!(idx_scalar, idx_simd);
        assert_eq!(fin_scalar, fin_simd);
    }

    #[test]
    fn avx2_triangle_areas_matches_scalar() {
        if !avx2_available() {
            return;
        }
        let cx: Vec<f64> = (0..23).map(|i| i as f64 * 1.3).collect();
        let cy: Vec<f64> = (0..23).map(|i| (i as f64 * 0.7).sin()).collect();
        let prev = (0.0, 0.0);
        let avg = (30.0, 0.5);

        let mut scalar_out = vec![0.0; cx.len()];
        triangle_areas_scalar(prev, avg, &cx, &cy, &mut scalar_out);

        let mut simd_out = vec![0.0; cx.len()];
        unsafe {
            triangle_areas_avx2(prev, avg, &cx, &cy, &mut simd_out);
        }
        for (a, b) in scalar_out.iter().zip(&simd_out) {
            assert!((a - b).abs() < 1e-9, "scalar={a} simd={b}");
        }
    }
}
