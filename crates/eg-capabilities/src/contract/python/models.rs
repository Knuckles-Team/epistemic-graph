//! EH-192 — strict nested Pydantic models for every method and every nested request
//! and result type, in one generated module (`epistemic_graph/generated/models.py`).
//!
//! Before this, only the methods listed in `DTO_SURFACES` had nested models: every
//! other request typed its nested fields as `Any` and returned an `OpaqueResult`.
//! Here every definition the request document and the result documents declare —
//! `Method` itself included, so a nested `MutationOperation.method` is a real
//! discriminated union — is rendered once, `extra="forbid"`, after [`hoist`] has named
//! every anonymous object and variant. A domain module then:
//!
//! * binds `{Id}Request` to the method's `Method{Id}Params` model (DTO surfaces keep
//!   their own request classes); and
//! * gains `decode_{method}(result)` for every schematized single-body result that
//!   still returns an `OpaqueResult`, validating the payload against the declared
//!   model and raising `ContractViolation` on a mismatch.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde_json::{Map, Value};

use super::super::results::{Catalog, Declared};
use super::dto::ref_name;
use super::dto_surfaces::DTO_SURFACES;
use super::{definition_origin, error_code_definitions};

mod hoist;
mod render;

pub(super) use render::{member_classes, render_owned};

/// The module attribute a domain module binds the models module to.
pub(super) const MODELS_ALIAS: &str = "_models";
/// Result encodings whose payload is a schematized value a model can validate.
const MODELLED_ENCODINGS: &[(&str, bool)] = &[("Raw", false), ("Json", false), ("RawOrNull", true)];

/// The model a method's single result body validates against.
pub(super) struct ResultModel {
    name: String,
    nullable: bool,
}

/// Every generated model, plus which model each method's request and result use.
pub(super) struct ModelSpace {
    pub(super) definitions: Map<String, Value>,
    params: BTreeMap<String, String>,
    results: BTreeMap<String, ResultModel>,
}

impl ModelSpace {
    /// Merge the request document's and every result domain's definitions (they must
    /// agree), then hoist anonymous structure until every node is named.
    pub(super) fn build(document: &Value, catalog: &Catalog) -> Self {
        let definitions = merged_definitions(document, catalog);
        let mut hoister = hoist::Hoister::new(definitions);
        let results = result_models(&mut hoister, catalog);
        let definitions = hoister.run();
        let params = method_params(&definitions);
        Self {
            definitions,
            params,
            results,
        }
    }

    /// The params model a non-DTO method's `{Id}Request` resolves to. Only the
    /// conventional `Method{Id}Params` name is bound: the domain module's lazy
    /// resolver derives the model name from the request name.
    pub(super) fn request_model(&self, id: &str) -> Option<&str> {
        if DTO_SURFACES.iter().any(|surface| surface.method == id) {
            return None;
        }
        let model = self.params.get(id)?;
        (*model == request_model_name(id)).then_some(model.as_str())
    }

    /// The model `decode_{method}` validates against, and whether it may be null.
    pub(super) fn result_model(&self, id: &str) -> Option<(&str, bool)> {
        let model = self.results.get(id)?;
        Some((model.name.as_str(), model.nullable))
    }
}

/// The request `$defs` (which name the `Method` root) plus every result domain's
/// definitions. One definition per name: a disagreement is a generator defect.
fn merged_definitions(document: &Value, catalog: &Catalog) -> Map<String, Value> {
    let mut definitions = document
        .get("$defs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (domain, domain_definitions) in &catalog.definitions {
        for (name, definition) in domain_definitions {
            if let Some(previous) = definitions.insert(name.clone(), definition.clone()) {
                assert_eq!(
                    &previous,
                    definition,
                    "two distinct Rust types share the schema definition name {name} \
                     (first: {}; result domain {domain}: {}); give one a \
                     domain-specific name",
                    definition_origin(&previous),
                    definition_origin(definition),
                );
            }
        }
    }
    definitions.extend(error_code_definitions());
    definitions
}

/// Register one named model per schematized single-body result: the referenced
/// definition, or a hoisted `{Id}Result` for an inline body.
fn result_models(hoister: &mut hoist::Hoister, catalog: &Catalog) -> BTreeMap<String, ResultModel> {
    let mut out = BTreeMap::new();
    for (id, declared) in &catalog.methods {
        let Some((schema, nullable)) = modelled_body(declared) else {
            continue;
        };
        let name = match ref_name(schema) {
            Some(name) => name.to_string(),
            None => {
                let reference = hoister.register(&format!("{id}Result"), schema.clone());
                ref_name(&reference)
                    .expect("a registered model")
                    .to_string()
            }
        };
        out.insert((*id).to_string(), ResultModel { name, nullable });
    }
    out
}

/// The schema of a single, non-dynamic body in a modelled encoding.
fn modelled_body(declared: &Declared) -> Option<(&Value, bool)> {
    let body = declared.single()?;
    if body.dynamic.is_some() {
        return None;
    }
    let (_, nullable) = MODELLED_ENCODINGS
        .iter()
        .find(|(encoding, _)| *encoding == body.encoding)?;
    Some((&body.schema, *nullable))
}

/// `method tag -> params model` from the hoisted `Method` union's variants.
fn method_params(definitions: &Map<String, Value>) -> BTreeMap<String, String> {
    let variants = definitions
        .get("Method")
        .and_then(|method| method.get("oneOf").or_else(|| method.get("anyOf")))
        .and_then(Value::as_array)
        .expect("the request document names its Method root");
    variants
        .iter()
        .filter_map(|variant| {
            let properties = variant.get("properties")?;
            let tag = properties.get("method")?.get("const")?.as_str()?;
            let params = ref_name(properties.get("params")?)?;
            Some((tag.to_string(), params.to_string()))
        })
        .collect()
}

/// The conventional params model of a method's request.
fn request_model_name(id: &str) -> String {
    format!("Method{id}Params")
}

/// A `def` head in the formatter's form: one line when it fits, else one parameter
/// per line with a magic trailing comma.
pub(super) fn push_def_head(out: &mut String, name: &str, parameters: &[&str], returns: &str) {
    let flat = format!("def {name}({}) -> {returns}:", parameters.join(", "));
    if flat.len() <= LINE_LIMIT {
        let _ = writeln!(out, "{flat}");
        return;
    }
    let _ = writeln!(out, "def {name}(");
    for parameter in parameters {
        let _ = writeln!(out, "    {parameter},");
    }
    let _ = writeln!(out, ") -> {returns}:");
}

/// A call statement `{indent}{prefix}{callee}(args)` in the formatter's form: flat,
/// the arguments hugged on one indented line, or one per line with a trailing comma.
fn push_call(out: &mut String, indent: &str, callee: &str, arguments: &[&str]) {
    let hugged = arguments.join(", ");
    let flat = format!("{indent}{callee}({hugged})");
    if flat.len() <= LINE_LIMIT {
        let _ = writeln!(out, "{flat}");
    } else if indent.len() + 4 + hugged.len() <= LINE_LIMIT {
        let _ = writeln!(out, "{indent}{callee}(\n{indent}    {hugged}\n{indent})");
    } else {
        let _ = writeln!(out, "{indent}{callee}(");
        for argument in arguments {
            let _ = writeln!(out, "{indent}    {argument},");
        }
        let _ = writeln!(out, "{indent})");
    }
}

/// The lazy request validation a non-DTO send runs: the models module is imported
/// on the first send, never when the domain module is.
///
/// A `native_field` (see [`super::NATIVE_PREPARED_FIELDS`]) may instead carry the
/// client's native-checked bytes for that field. The native codec already
/// validated them, and the client builds the wire body from them, so the model
/// check applies only to the structured form.
pub(super) fn push_lazy_request_validation(
    out: &mut String,
    model: &str,
    native_field: Option<&str>,
) {
    let callee = format!("models().{model}.model_validate");
    match native_field {
        Some(field) => {
            let _ = writeln!(
                out,
                "    if not isinstance((params or {{}}).get({field:?}), bytes):"
            );
            push_call(out, "        ", &callee, &["params or {}"]);
        }
        None => push_call(out, "    ", &callee, &["params or {}"]),
    }
}

/// `decode_{name}(result)` validating an `OpaqueResult` against its declared model.
/// The annotation names `_models` (imported only for type checkers); the body
/// resolves the model through `models()`, so decoding is what loads the module.
pub(super) fn push_decode(out: &mut String, id: &str, name: &str, model: (&str, bool)) {
    let (model, nullable) = model;
    let suffix = if nullable { " | None" } else { "" };
    let annotation = format!("{MODELS_ALIAS}.{model}{suffix}");
    let runtime = format!("models().{model}{suffix}");
    let function = format!("decode_{name}");
    out.push_str("\n\n");
    push_def_head(out, &function, &["result: OpaqueResult"], &annotation);
    out.push_str("    \"\"\"Validate this method's result against its contract model.\"\"\"\n");
    let id = format!("{id:?}");
    push_call(
        out,
        "    ",
        "return decode_result",
        &[&id, &runtime, "result"],
    );
}

/// The domain module's lazy `{Id}Request` resolver: a module `__getattr__` over the
/// methods whose request is a generated model (PEP 562).
pub(super) fn push_request_resolver(out: &mut String, methods: &[&str]) {
    if methods.is_empty() {
        return;
    }
    out.push_str(
        "\n\n# Methods whose `{Id}Request` resolves to `models.Method{Id}Params` (EH-192).\n",
    );
    out.push_str("_REQUEST_METHODS = frozenset(\n    {\n");
    for method in methods {
        let _ = writeln!(out, "        \"{method}\",");
    }
    out.push_str("    }\n)\n\n\n");
    out.push_str(REQUEST_RESOLVER);
}

const REQUEST_RESOLVER: &str = r#"def __getattr__(name: str) -> Any:
    """Resolve a ``{Id}Request`` name to its generated model on first use."""
    method = name.removesuffix("Request")
    if name == method or method not in _REQUEST_METHODS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(models(), f"Method{method}Params")
"#;

/// The imports only a type checker reads: the models module and each lazily
/// resolved `{Id}Request` alias, so `from domain import XRequest` stays typed.
pub(super) fn push_type_checking_block(out: &mut String, methods: &[&str], body: &str) {
    if methods.is_empty() && !body.contains(&format!("{MODELS_ALIAS}.")) {
        return;
    }
    // The import block above always ends in one blank line: exactly what the
    // formatter wants before a top-level `if`.
    out.push_str("if TYPE_CHECKING:\n");
    let _ = writeln!(out, "    from . import models as {MODELS_ALIAS}");
    if !methods.is_empty() {
        out.push('\n');
    }
    for method in methods {
        let target = format!("{MODELS_ALIAS}.{}", request_model_name(method));
        let line = format!("    {method}Request = {target}");
        if line.len() <= LINE_LIMIT {
            let _ = writeln!(out, "{line}");
        } else {
            let _ = writeln!(out, "    {method}Request = (\n        {target}\n    )");
        }
    }
}

/// The formatter's line limit.
const LINE_LIMIT: usize = 88;

#[cfg(test)]
mod tests;
