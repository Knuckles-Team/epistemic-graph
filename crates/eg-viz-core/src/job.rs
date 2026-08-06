//! The `viz` job type (D-VZ-1 lane V0) — fits alongside `eg-jobs`'s existing
//! durable analytics-job plane (CONCEPT:INT-P2-1); this crate does **not** invent
//! a parallel job system.
//!
//! `eg-jobs::AnalyticsJob` already generalizes over "an algorithm, run with some
//! params, over an immutable input snapshot, tracked through
//! `submitted -> running -> publishing -> succeeded|failed|cancelled`" — see
//! `crates/eg-jobs/src/model.rs`. A render is exactly that shape: the algorithm is
//! "resolve this `ViewSpec` against this dataset at this tier", the params are the
//! spec itself, and the result is a [`crate::result::ViewResult`] instead of a
//! `TypedJobResult` (a render's output is tile/payload references + LOD metadata,
//! not KnowledgeBatch rows — `eg_jobs::result::TypedJobResult::validate` requires
//! mandatory epistemic columns like `confidence`/`evidence_refs` that a chart
//! render has no use for).
//!
//! So "viz" is a **job family within the existing plane**, not a new store: this
//! module supplies the family/algorithm naming and the params-digest function a V4
//! caller feeds into `eg_jobs::AlgoVersion` and `eg_jobs::InputSnapshotHandle`
//! directly — see the `dev-dependencies`-only interop test at the bottom of this
//! file, which builds a real `eg_jobs::AlgoVersion` from these values and computes
//! a real `eg_jobs::compute_result_ref` from it. This crate itself stays a leaf
//! (no eg-jobs/eg-types/eg-core runtime dependency, per the V0 "Depends on: —"
//! charter row) — the interop is proven at test time, not paid for at link time.

use sha2::{Digest, Sha256};

use crate::spec::ViewSpec;

/// The `AlgoVersion::family` value every viz job uses, mirroring how
/// `eg-compute::mining`'s families are named (`"mining.association"`,
/// `"mining.cluster"`, …).
pub const VIZ_JOB_FAMILY: &str = "viz.render";

/// The `AlgoVersion::algorithm` value for a given mark kind — one render job per
/// mark today; a multi-mark `ViewSpec` (several layered marks in one panel) is
/// still one job, named after its first/primary mark, since the job is "resolve
/// this whole spec," not "resolve one mark."
pub fn viz_algorithm_name(mark: crate::spec::MarkKind) -> &'static str {
    use crate::spec::MarkKind::*;
    match mark {
        Line => "line",
        Scatter => "scatter",
        Bar => "bar",
        Area => "area",
        Heatmap => "heatmap",
        Graph => "graph",
    }
}

/// An error digesting a [`ViewSpec`] — only reachable if `serde_json` itself
/// fails, which does not happen for this crate's own derived types.
#[derive(Debug)]
pub struct SpecDigestError(String);

impl std::fmt::Display for SpecDigestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to digest ViewSpec: {}", self.0)
    }
}

impl std::error::Error for SpecDigestError {}

/// A stable digest of a [`ViewSpec`] — the render-plane analogue of
/// `eg_jobs::digest_params`, which a caller feeds into
/// `AlgoVersion::params_digest`. Two byte-identical specs always digest to the
/// same value; the digest is over canonical JSON, so field order in the source
/// never matters (`serde_json` produces the same map key order for the same
/// struct's derived `Serialize` regardless of construction order).
pub fn spec_digest(spec: &ViewSpec) -> Result<String, SpecDigestError> {
    let json = serde_json::to_string(spec).map_err(|e| SpecDigestError(e.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"eg-viz-core.spec_digest.v1\0");
    hasher.update(json.as_bytes());
    Ok(hex::encode(&hasher.finalize()[..16]))
}

/// A stable content-addressed identifier for "this exact render": the spec, the
/// dataset it ran against, and the graph snapshot version it was pinned to — the
/// render-plane analogue of `eg_jobs::compute_result_ref`. Two identical render
/// requests over the same snapshot converge on the same `query_hash`, which is
/// what [`crate::result::ViewResult::query_hash`] carries and a tile cache (V4)
/// keys on.
pub fn query_hash(
    spec: &ViewSpec,
    dataset_ref: &str,
    snapshot_version: u64,
) -> Result<String, SpecDigestError> {
    let spec_digest = spec_digest(spec)?;
    let mut hasher = Sha256::new();
    hasher.update(b"eg-viz-core.query_hash.v1\0");
    hasher.update(spec_digest.as_bytes());
    hasher.update([0u8]);
    hasher.update(dataset_ref.as_bytes());
    hasher.update([0u8]);
    hasher.update(snapshot_version.to_le_bytes());
    Ok(format!(
        "eg:viz_query:{}",
        hex::encode(&hasher.finalize()[..16])
    ))
}

/// Everything a V4 caller needs to submit a render as an `eg_jobs::AnalyticsJob`:
/// the spec, which dataset/snapshot it runs against, and the frame budget the tier
/// selection ([`crate::tier::select_tier`]) resolves against. This struct is the
/// crate-local, dependency-free stand-in for what becomes an
/// `AnalyticsJob.input_payload` (opaque bytes) plus an `AlgoVersion` once a V4
/// caller adapts it — seeing that adaptation is exactly two struct literals is
/// what proves "fits the existing idiom" (see the interop test below).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ViewJobRequest {
    pub spec: ViewSpec,
    pub dataset_ref: String,
    pub snapshot_version: u64,
    pub budget: crate::tier::FrameBudget,
}

impl ViewJobRequest {
    pub fn new(
        spec: ViewSpec,
        dataset_ref: impl Into<String>,
        snapshot_version: u64,
        budget: crate::tier::FrameBudget,
    ) -> Self {
        Self {
            spec,
            dataset_ref: dataset_ref.into(),
            snapshot_version,
            budget,
        }
    }

    /// The `AlgoVersion::family`/`algorithm` this request would run under. The
    /// primary mark is the first in `spec.marks` — see [`viz_algorithm_name`] doc.
    pub fn algorithm_name(&self) -> &'static str {
        self.spec
            .marks
            .first()
            .map(|m| viz_algorithm_name(m.kind))
            .unwrap_or("empty")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Encodings, MarkKind, MarkSpec};

    fn sample_spec() -> ViewSpec {
        ViewSpec::new(vec![MarkSpec::new(MarkKind::Scatter, "ds:1")
            .with_encodings(Encodings {
                x: Some(crate::spec::EncodingSpec::field("x")),
                y: Some(crate::spec::EncodingSpec::field("y")),
                ..Default::default()
            })])
    }

    #[test]
    fn spec_digest_is_deterministic() {
        let spec = sample_spec();
        assert_eq!(spec_digest(&spec).unwrap(), spec_digest(&spec).unwrap());
    }

    #[test]
    fn spec_digest_changes_when_the_spec_changes() {
        let a = sample_spec();
        let mut b = sample_spec();
        b.title = Some("different".to_string());
        assert_ne!(spec_digest(&a).unwrap(), spec_digest(&b).unwrap());
    }

    #[test]
    fn query_hash_is_deterministic_and_dataset_sensitive() {
        let spec = sample_spec();
        let h1 = query_hash(&spec, "ds:1", 7).unwrap();
        let h2 = query_hash(&spec, "ds:1", 7).unwrap();
        let h3 = query_hash(&spec, "ds:2", 7).unwrap();
        let h4 = query_hash(&spec, "ds:1", 8).unwrap();
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        assert_ne!(h1, h4);
        assert!(h1.starts_with("eg:viz_query:"));
    }

    #[test]
    fn view_job_request_names_its_primary_mark_algorithm() {
        let budget = crate::tier::FrameBudget::new(1_000_000, 1_000_000);
        let request = ViewJobRequest::new(sample_spec(), "ds:1", 1, budget);
        assert_eq!(request.algorithm_name(), "scatter");
    }

    /// The interop proof: `eg-viz-core`'s job-shaped values plug into the REAL
    /// `eg_jobs::AlgoVersion`/`InputSnapshotHandle`/`compute_result_ref` with no
    /// adapter type — "fits alongside the existing analytics job plane" is a
    /// tested fact. `eg-jobs` is a dev-dependency only (see this crate's
    /// `Cargo.toml`), so this proof costs nothing in a normal/default build of
    /// `eg-viz-core`.
    #[test]
    fn viz_job_values_bind_directly_into_eg_jobs_algo_version_and_result_ref() {
        let spec = sample_spec();
        let request = ViewJobRequest::new(
            spec.clone(),
            "ds:1",
            3,
            crate::tier::FrameBudget::new(2_000_000, 16 * 1024 * 1024),
        );

        let algo = eg_jobs::AlgoVersion {
            family: VIZ_JOB_FAMILY.to_string(),
            algorithm: request.algorithm_name().to_string(),
            params_digest: spec_digest(&spec).unwrap(),
            code_version: "test".to_string(),
            env_version: "test".to_string(),
        };
        let snapshot = eg_jobs::InputSnapshotHandle::new("g:test", request.snapshot_version)
            .with_dataset(request.dataset_ref.clone(), "content-digest-placeholder");

        let result_ref = eg_jobs::compute_result_ref(&snapshot, &algo);
        assert!(result_ref.starts_with("eg:job_result:"));

        // Same inputs -> same result_ref (eg-jobs' own idempotency contract, which
        // this binding inherits for free).
        let result_ref_again = eg_jobs::compute_result_ref(&snapshot, &algo);
        assert_eq!(result_ref, result_ref_again);
    }
}
