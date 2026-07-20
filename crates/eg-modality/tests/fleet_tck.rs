//! EG-P1-1 fleet aggregator — the ONE place the whole-fleet modality TCK parity is
//! machine-visible.
//!
//! ## Why a test, not a `src/bin/` binary
//!
//! True cross-crate aggregation needs every modality's `ModalityContract` impl to
//! exist in ONE process so its `tck_report` can be collected. `eg-modality` itself
//! canNOT depend on the pilot crates (`eg-tensor`/`eg-geo`) in `[dependencies]` — that
//! would invert the workspace DAG (they depend on IT). A `src/bin/` target links
//! against `[dependencies]` only, so a binary in this crate could never see the
//! pilots. A *test* target, however, links against `[dev-dependencies]`, and Cargo
//! explicitly permits a DEV-dependency cycle — so this integration test pulls
//! `eg-tensor`/`eg-geo` (with their `contract` feature) in as dev-deps, giving every
//! pilot's `impl ModalityContract` + `impl ConformanceTestable` a home in this one
//! test process. That is the documented mechanism the EG-P1-1 directive calls for.
//!
//! Running `cargo test -p eg-modality --test fleet_tck -- --nocapture` prints the
//! combined markdown parity table (also the source of the table in `README.md`).
//! `cargo build -p eg-modality` and every crate that depends on eg-modality are
//! unaffected by the dev-dep cycle.

use eg_audio::AudioData;
use eg_document::DocumentData;
use eg_geo::Geometry;
use eg_image::ImageData;
use eg_modality::{
    register_modality, registered_modalities, render_fleet_table, tck_report, ModalityDescriptor,
    TckReport,
};
use eg_tensor::Tensor;
use eg_video::VideoData;

/// Collect the pilots' TCK reports, register them into the shared runtime registry,
/// and assert the whole-fleet first-class picture. Emits the combined parity table.
#[test]
fn fleet_parity_table_and_first_class_pilots() {
    // Register each pilot exactly as the conformance macro does — proving the same
    // registry sees BOTH modalities once they share a process (unlike each crate's own
    // per-process test run, which only ever registers itself).
    register_modality(ModalityDescriptor {
        name: "tensor",
        tck_report: tck_report::<Tensor>,
    });
    register_modality(ModalityDescriptor {
        name: "geo",
        tck_report: tck_report::<Geometry>,
    });
    register_modality(ModalityDescriptor {
        name: "document",
        tck_report: tck_report::<DocumentData>,
    });
    register_modality(ModalityDescriptor {
        name: "image",
        tck_report: tck_report::<ImageData>,
    });
    register_modality(ModalityDescriptor {
        name: "audio",
        tck_report: tck_report::<AudioData>,
    });
    register_modality(ModalityDescriptor {
        name: "video",
        tck_report: tck_report::<VideoData>,
    });

    // Both pilots must be discoverable through the ONE registry in this process.
    let registered = registered_modalities();
    for name in ["tensor", "geo", "document", "image", "audio", "video"] {
        assert!(
            registered.iter().any(|d| d.name == name),
            "modality {name:?} must be registered in the shared fleet registry"
        );
    }

    // Build the fleet table from every registered modality's live report (recomputed
    // via the stored `tck_report` fn pointer — the registry is the single source).
    let mut reports: Vec<TckReport> = registered.iter().map(|d| (d.tck_report)()).collect();
    reports.sort_by(|a, b| a.modality.cmp(b.modality));

    let table = render_fleet_table(&reports);
    println!("\n{table}");

    // Every pilot is honestly first-class (no NotImplemented gap remains — points a
    // modality genuinely can't own are N/A with a reason, not a gap).
    for report in &reports {
        assert!(
            report.is_first_class(),
            "modality {:?} is not first-class: {}",
            report.modality,
            report.summary()
        );
    }

    // Direct, non-table assertions on the two pilots so a regression fails loudly.
    let tensor = tck_report::<Tensor>();
    assert_eq!(tensor.covered_count(), 12, "tensor: {}", tensor.summary());
    assert_eq!(tensor.pass_count(), 8, "tensor: {}", tensor.summary());
    assert_eq!(tensor.na_count(), 4, "tensor: {}", tensor.summary());

    let geo = tck_report::<Geometry>();
    assert_eq!(geo.covered_count(), 12, "geo: {}", geo.summary());
    assert_eq!(geo.pass_count(), 8, "geo: {}", geo.summary());
    assert_eq!(geo.na_count(), 4, "geo: {}", geo.summary());

    // Served production modalities have no N/A escape hatch: every core dimension
    // executes and passes.
    for report in [
        tck_report::<DocumentData>(),
        tck_report::<ImageData>(),
        tck_report::<AudioData>(),
        tck_report::<VideoData>(),
    ] {
        assert!(report.is_production_ready(), "{}", report.summary());
        assert!(report.native_probe_passed(), "{}", report.summary());
        assert_eq!(report.pass_count(), 12, "{}", report.summary());
        assert_eq!(report.na_count(), 0, "{}", report.summary());
    }
}
