use std::collections::BTreeSet;

use super::super::models::ModelSpace;
use super::*;
use crate::contract::results::Catalog;
use crate::contract::schema::method_request_document;

fn space() -> ModelSpace {
    ModelSpace::build(&method_request_document(), &Catalog::collect())
}

/// The names a module text declares at top level (`class X(` / `X = `).
fn declared(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| {
            let name = line
                .strip_prefix("class ")
                .map(|rest| rest.split('(').next().unwrap_or(rest))
                .or_else(|| line.split_once(" = ").map(|(name, _)| name))?;
            let identifier = name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            (identifier && !name.is_empty() && name != "__all__").then(|| name.to_string())
        })
        .collect()
}

/// EH-377: every wire shape is ONE class in the whole generated package -- the
/// surface modules and `models.py` no longer render the same definition twice.
#[test]
fn every_definition_is_rendered_by_exactly_one_module() {
    let space = space();
    let surfaces = Surfaces::new(&space.definitions);
    let mut seen = BTreeSet::new();
    for name in declared(&surfaces.package_text()) {
        assert!(seen.insert(name.clone()), "{name} is rendered twice");
    }
    for name in space.definitions.keys() {
        assert!(seen.contains(name), "{name} is rendered by no module");
    }
}

/// A surface module exposes every definition its roots reach, and `models.py`
/// exposes every definition, each rendered once and re-exported elsewhere.
#[test]
fn modules_keep_their_whole_surface_through_reexports() {
    let space = space();
    let surfaces = Surfaces::new(&space.definitions);
    let connector_pack = DTO_SURFACES
        .iter()
        .find(|surface| surface.module == "connector_pack")
        .expect("connector_pack surface");
    let dto = surfaces.dto_module(connector_pack);
    let models = surfaces.models_module();
    assert!(dto.contains("\nclass PackAnnotations(BaseModel):"));
    assert!(!models.contains("\nclass PackAnnotations("));
    assert!(models.contains("from .connector_pack import (\n"));
    assert!(models.contains("\n    \"PackAnnotations\",\n"));
    // `Digest256` is reached by several surfaces: `_shared` renders it once and the
    // surface module re-exports it.
    assert_eq!(surfaces.owner("Digest256"), SHARED_MODULE);
    assert!(surfaces.shared_module().contains("\nDigest256 = "));
    assert!(dto.contains("from ._shared import (\n"));
    assert!(dto.contains("\n    \"Digest256\",\n"));
    assert!(!dto.contains("\nDigest256 = "));
}

/// Surface modules import only `_shared` (and the digest helpers); `_shared`
/// imports no other generated module; nothing but `models.py` names another surface.
#[test]
fn the_module_import_graph_is_acyclic() {
    let space = space();
    let surfaces = Surfaces::new(&space.definitions);
    let relative = |text: &str| -> BTreeSet<String> {
        text.lines()
            .filter_map(|line| line.strip_prefix("from ."))
            .filter_map(|rest| rest.split(' ').next())
            .map(str::to_string)
            .collect()
    };
    let allowed: BTreeSet<String> = [SHARED_MODULE, "digest"].map(str::to_string).into();
    for surface in DTO_SURFACES
        .iter()
        .filter(|surface| surfaces.generates(surface))
    {
        let imports = relative(&surfaces.dto_module(surface));
        assert!(
            imports.is_subset(&allowed),
            "{}: {imports:?}",
            surface.module
        );
    }
    let shared = relative(&surfaces.shared_module());
    assert!(shared.iter().all(|module| module == "digest"), "{shared:?}");
}

/// Surface and `_shared` modules complete their classes at import (resolved
/// `model_fields` annotations, as before EH-377); `models.py` stays lazy.
#[test]
fn surface_modules_complete_their_classes_and_models_stays_lazy() {
    let space = space();
    let surfaces = Surfaces::new(&space.definitions);
    let rdf_report = DTO_SURFACES
        .iter()
        .find(|surface| surface.module == "rdf_report")
        .expect("rdf_report surface");
    assert!(surfaces
        .dto_module(rdf_report)
        .contains("\nOwlReasonResult.model_rebuild()\n"));
    assert!(surfaces.shared_module().contains(".model_rebuild()\n"));
    assert!(!surfaces.models_module().contains(".model_rebuild()"));
}

/// A module that re-exports a shared tagged union re-exports its variant classes
/// too, and `models.py` exposes them: consumers construct and `isinstance`-check
/// `ComponentProvenanceMcpServer` from `agent_component` (graph-os) and the decision
/// variants from `decision` (AU), as they could before EH-377 moved the union.
#[test]
fn a_reexported_union_carries_its_variant_classes() {
    let space = space();
    let surfaces = Surfaces::new(&space.definitions);
    let module = |name: &str| {
        let surface = DTO_SURFACES
            .iter()
            .find(|surface| surface.module == name)
            .unwrap_or_else(|| panic!("{name} surface"));
        surfaces.dto_module(surface)
    };
    let models = surfaces.models_module();
    for (surface, class) in [
        ("agent_component", "ComponentProvenanceMcpServer"),
        ("decision", "AbstainReasonInsufficientConfidence"),
        ("decision", "CandidateSourceRecordAgentLibrary"),
        ("decision", "DecisionOutcomeAbstained"),
    ] {
        let text = module(surface);
        let listed = format!("\n    \"{class}\",\n");
        assert!(text.contains(&listed), "{surface} does not export {class}");
        assert!(models.contains(&listed), "models does not export {class}");
    }
}
