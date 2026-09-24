use std::collections::BTreeSet;

use super::super::surfaces::Surfaces;
use super::*;
use crate::contract::results::Catalog;
use crate::contract::schema::method_request_document;
use crate::ConsumerProfile;

fn space() -> (Value, Catalog, ModelSpace) {
    let document = method_request_document();
    let catalog = Catalog::collect();
    let space = ModelSpace::build(&document, &catalog);
    (document, catalog, space)
}

fn contains_root_ref(node: &Value) -> bool {
    match node {
        Value::Object(object) => {
            object.get("$ref").and_then(Value::as_str) == Some("#")
                || object.values().any(contains_root_ref)
        }
        Value::Array(values) => values.iter().any(contains_root_ref),
        _ => false,
    }
}

/// The request document names its `Method` root and never refers to it as `#`, so
/// `MutationOperation.method` is the same `$ref` in the request and result documents.
#[test]
fn the_request_document_names_its_method_root() {
    let (document, catalog, _) = space();
    assert!(document["$defs"].get("Method").is_some());
    assert!(!contains_root_ref(&document));
    let request = &document["$defs"]["MutationOperation"];
    for definitions in catalog.definitions.values() {
        if let Some(result) = definitions.get("MutationOperation") {
            assert_eq!(
                request, result,
                "MutationOperation disagrees across documents"
            );
        }
    }
}

/// Every published method whose request has fields binds a params model, and that
/// model is rendered.
#[test]
fn every_published_method_with_fields_binds_a_rendered_params_model() {
    let (document, _, space) = space();
    let rendered = Surfaces::new(&space.definitions).package_text();
    for descriptor in crate::method_descriptors() {
        let id = descriptor.id.as_str();
        let has_params = document["methods"][id]["properties"]
            .get("params")
            .is_some();
        if !descriptor.serves(ConsumerProfile::PythonClient) || !has_params {
            continue;
        }
        let dto = DTO_SURFACES.iter().any(|surface| surface.method == id);
        let Some(model) = space.request_model(id) else {
            assert!(dto, "{id} has request fields but no params model");
            continue;
        };
        assert!(
            rendered.contains(&format!("\nclass {model}(BaseModel):")),
            "{id}: {model} is not rendered"
        );
    }
}

/// Every schematized single-body result gets a model, and that model is rendered.
#[test]
fn schematized_results_name_rendered_models() {
    let (_, catalog, space) = space();
    let rendered = Surfaces::new(&space.definitions).package_text();
    for (id, declared) in &catalog.methods {
        let Some((name, _)) = space.result_model(id) else {
            assert!(
                modelled_body(declared).is_none(),
                "{id} lost its result model"
            );
            continue;
        };
        let declared_name = [format!("\nclass {name}("), format!("\n{name} = ")];
        assert!(
            declared_name
                .iter()
                .any(|needle| rendered.contains(needle.as_str())),
            "{id}: result model {name} is not rendered"
        );
    }
}

/// A domain module never imports the models module when it is imported: requests
/// resolve through `__getattr__` and results through `models()` on first use.
#[test]
fn domain_modules_load_models_lazily() {
    let catalog = Catalog::collect();
    for artifact in super::super::artifacts(&catalog) {
        let text = String::from_utf8(artifact.bytes).expect("UTF-8 Python");
        if artifact.path.ends_with("/models.py") || !artifact.path.contains("/generated/") {
            continue;
        }
        let eager = text
            .lines()
            .any(|line| line.starts_with("from . import models") || line == "import models");
        assert!(!eager, "{} imports models at module load", artifact.path);
    }
}

/// Every rendered class name is unique across the package, and the `Method` root is
/// a real union.
#[test]
fn hoisting_names_every_object_once() {
    let (_, _, space) = space();
    let rendered = Surfaces::new(&space.definitions).package_text();
    let mut names = BTreeSet::new();
    for line in rendered.lines().filter(|line| line.starts_with("class ")) {
        let name = line["class ".len()..]
            .split('(')
            .next()
            .expect("class name");
        assert!(
            names.insert(name.to_string()),
            "class {name} is rendered twice"
        );
    }
    assert!(names.contains("MethodSparqlParams"));
    assert!(rendered.contains("\nMethod = Annotated["));
}

/// The package never lists `models` among its eager imports.
#[test]
fn the_package_does_not_import_models_eagerly() {
    let catalog = Catalog::collect();
    let init = super::super::artifacts(&catalog)
        .into_iter()
        .find(|artifact| artifact.path == "epistemic_graph/generated/__init__.py")
        .expect("generated package init");
    let text = String::from_utf8(init.bytes).expect("UTF-8 Python");
    assert!(
        !text.contains("\n    models,\n"),
        "__init__ imports models eagerly"
    );
}

/// The generated tree is ruff-clean and formatter-stable under the repository's
/// own ruff settings (the version `.config/pre-commit.yaml` pins must be on PATH).
/// This is the proof the renderer's "formatter-stable by construction" rules hold.
#[test]
fn generated_python_is_ruff_clean_and_formatter_stable() {
    use std::process::Command;

    let catalog = Catalog::collect();
    let root = std::env::temp_dir().join(format!("eg-generated-ruff-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    for artifact in super::super::artifacts(&catalog) {
        let path = root.join(&artifact.path);
        std::fs::create_dir_all(path.parent().expect("artifact directory")).expect("mkdir");
        std::fs::write(path, artifact.bytes).expect("write generated artifact");
    }
    std::fs::write(
        root.join("pyproject.toml"),
        "[tool.ruff]\nline-length = 88\ntarget-version = \"py310\"\n\n\
         [tool.ruff.lint]\nselect = [\"E\", \"F\", \"I\", \"UP\", \"B\", \"RUF100\"]\n",
    )
    .expect("write ruff settings");
    for arguments in [
        &["check", "--no-cache", "epistemic_graph"][..],
        &["format", "--check", "--no-cache", "epistemic_graph"][..],
    ] {
        let output = Command::new("ruff")
            .args(arguments)
            .current_dir(&root)
            .output()
            .expect("ruff is on PATH (the version .config/pre-commit.yaml pins)");
        assert!(
            output.status.success(),
            "ruff {arguments:?} failed:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
