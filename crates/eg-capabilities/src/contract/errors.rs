//! Published engine error vocabulary. The typed enum families are the source
//! of the codes; this module supplies transport metadata and method routing.

use std::collections::BTreeMap;

use eg_types::connector_pack::result::PackWriteErrorCode;
use eg_types::contract::{EngineErrorCode, ServerErrorCode, UqlCode};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::DecisionErrorCode;
use eg_types::graph_schema::GraphSchemaErrorCode;
use eg_types::solve::SolveErrorCode;

pub(super) use crate::error_routing::method_error_set;
use crate::error_routing::typed_codes;

fn families() -> [(&'static str, Vec<&'static str>); 8] {
    [
        (
            "engine",
            typed_codes(EngineErrorCode::ALL, EngineErrorCode::as_str),
        ),
        ("engine", typed_codes(UqlCode::ALL, UqlCode::as_str)),
        (
            "decision",
            typed_codes(DecisionErrorCode::ALL, DecisionErrorCode::as_str),
        ),
        (
            "decision",
            typed_codes(StatisticalErrorCode::ALL, StatisticalErrorCode::as_str),
        ),
        (
            "schema",
            typed_codes(GraphSchemaErrorCode::ALL, GraphSchemaErrorCode::as_str),
        ),
        (
            "connector",
            typed_codes(PackWriteErrorCode::ALL, PackWriteErrorCode::as_str),
        ),
        (
            "solve",
            typed_codes(SolveErrorCode::ALL, SolveErrorCode::as_str),
        ),
        (
            "server",
            typed_codes(ServerErrorCode::ALL, ServerErrorCode::as_str),
        ),
    ]
}

const CODE_CLASSES: &[(&str, &str)] = &[
    ("AUTH_TENANT_MISMATCH", "auth"),
    ("AUTH_AUDIENCE_MISMATCH", "auth"),
    ("AUTH_POLICY_VERSION_MISMATCH", "auth"),
    ("NODE_MISMATCH", "auth"),
    ("ACCESS_DENIED", "policy"),
    ("POLICY_NATIVE_AUTHORITY_REQUIRED", "policy"),
    ("CAPACITY_DENIED", "capacity"),
    ("ENGINE_RESOURCE_EXHAUSTED", "capacity"),
    ("UQL_BUDGET_EXCEEDED", "capacity"),
    ("REDIRECTED", "availability"),
    ("OPERATION_REDIRECTED", "availability"),
    ("ENGINE_UNAVAILABLE", "availability"),
    ("STALE_ROUTE", "availability"),
    ("ENGINE_DEADLINE_EXCEEDED", "availability"),
    ("CANCELLED", "availability"),
    ("TELEMETRY_UNAVAILABLE", "availability"),
    ("CAPACITY_UNAVAILABLE", "availability"),
    ("DECISIONS", "availability"),
    ("CONFLICT", "conflict"),
    ("STALE_OUTBOX_LEASE", "conflict"),
    ("IDEMPOTENCY_CONFLICT", "conflict"),
    ("READ_ONLY", "conflict"),
    ("AUTHENTICATION_REQUIRED", "auth"),
    ("BUSY", "capacity"),
    ("INTERNAL", "internal"),
    ("ELEVATION_ACTOR_UNSTAMPED", "internal"),
    ("GOVERNED_ACTOR_UNSTAMPED", "internal"),
    ("SCHEMA_INCONSISTENT", "schema"),
];

const FAMILY_CLASSES: &[(&str, &str)] = &[
    ("engine", "validation"),
    ("decision", "decision"),
    ("schema", "schema"),
    ("connector", "connector"),
    ("solve", "solve"),
    ("server", "server"),
];

fn class(code: &str, family: &str) -> &'static str {
    CODE_CLASSES
        .iter()
        .find_map(|(token, class)| (*token == code).then_some(*class))
        .or_else(|| {
            FAMILY_CLASSES
                .iter()
                .find_map(|(name, class)| (*name == family).then_some(*class))
        })
        .expect("closed error family")
}

fn retryable(code: &str) -> bool {
    matches!(
        code,
        "REDIRECTED"
            | "ENGINE_UNAVAILABLE"
            | "ENGINE_DEADLINE_EXCEEDED"
            | "ENGINE_RESOURCE_EXHAUSTED"
            | "CAPACITY_DENIED"
            | "READ_ONLY"
            | "BUSY"
            | "OPERATION_REDIRECTED"
            | "STALE_ROUTE"
            | "STALE_OUTBOX_LEASE"
    )
}

const HTTP_HINTS: &[(&str, u16)] = &[
    ("AUTH_TENANT_MISMATCH", 403),
    ("AUTH_AUDIENCE_MISMATCH", 403),
    ("AUTH_POLICY_VERSION_MISMATCH", 403),
    ("NODE_MISMATCH", 403),
    ("ACCESS_DENIED", 403),
    ("POLICY_NATIVE_AUTHORITY_REQUIRED", 403),
    ("REDIRECTED", 307),
    ("OPERATION_REDIRECTED", 307),
    ("ENGINE_UNAVAILABLE", 503),
    ("STALE_ROUTE", 503),
    ("STALE_OUTBOX_LEASE", 409),
    ("READ_ONLY", 503),
    ("ENGINE_DEADLINE_EXCEEDED", 504),
    ("CAPACITY_DENIED", 429),
    ("ENGINE_RESOURCE_EXHAUSTED", 429),
    ("UQL_BUDGET_EXCEEDED", 429),
    ("CONFLICT", 409),
    ("IDEMPOTENCY_CONFLICT", 409),
    ("AUTHENTICATION_REQUIRED", 401),
    ("BUSY", 429),
    ("TIMEOUT", 504),
    ("INTERNAL", 500),
    ("CANCELLED", 408),
    ("TELEMETRY_UNAVAILABLE", 503),
    ("CAPACITY_UNAVAILABLE", 503),
    ("DECISIONS", 503),
    ("ELEVATION_ACTOR_UNSTAMPED", 500),
    ("GOVERNED_ACTOR_UNSTAMPED", 500),
    ("SCHEMA_INCONSISTENT", 409),
    ("GRAPH_NOT_FOUND", 404),
    ("TELEMETRY_WINDOW_TOO_LARGE", 413),
];

fn http_status_hint(code: &str) -> u16 {
    HTTP_HINTS
        .iter()
        .find_map(|(token, hint)| (*token == code).then_some(*hint))
        .unwrap_or(400)
}

pub(super) fn catalog_json() -> Vec<u8> {
    let mut rows = BTreeMap::new();
    for (family, codes) in families() {
        for code in codes {
            let row = serde_json::json!({
                "code": code,
                "class": class(code, family),
                "retryable": retryable(code),
                "http_status_hint": http_status_hint(code),
            });
            if let Some(previous) = rows.insert(code, row.clone()) {
                assert_eq!(previous, row, "conflicting metadata for {code}");
            }
        }
    }
    super::pretty(&serde_json::json!({
        "contract_version": 1,
        "generator": "eg-capabilities/gen_contract",
        "errors": rows.into_values().collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::Path;

    fn server_sources(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("server source directory") {
            let path = entry.expect("server source entry").path();
            let name = path.file_name().unwrap().to_string_lossy();
            if name.contains("test") {
                continue;
            }
            if path.is_dir() {
                server_sources(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    use quote::ToTokens;
    use syn::visit::Visit;

    #[derive(Default)]
    struct FunctionRefusals {
        codes: BTreeSet<String>,
        references: BTreeSet<String>,
        wire_boundary: bool,
    }

    /// A conservative source census, not a compiler-resolved reachability proof.
    /// Equal names in different modules are merged: ambiguity adds coverage
    /// rather than hiding a possible wire refusal.
    #[derive(Default)]
    struct RefusalCensus {
        functions: BTreeMap<String, FunctionRefusals>,
        current: String,
    }

    fn test_only(meta: &syn::Meta) -> bool {
        match meta {
            syn::Meta::Path(path) => path.is_ident("test"),
            syn::Meta::List(list) => {
                let items = list
                    .parse_args_with(
                        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
                    )
                    .expect("cfg arguments");
                if list.path.is_ident("all") {
                    items.iter().any(test_only)
                } else if list.path.is_ident("any") {
                    !items.is_empty() && items.iter().all(test_only)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn test_attrs(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attr| {
            attr.path().is_ident("test")
                || (attr.path().is_ident("cfg")
                    && attr
                        .parse_args::<syn::Meta>()
                        .is_ok_and(|meta| test_only(&meta)))
        })
    }

    impl RefusalCensus {
        fn literal(&mut self, literal: &syn::LitStr) {
            let text = literal.value();
            if let Some((code, _)) = text.split_once(": ") {
                if code.len() >= 4
                    && code
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                {
                    self.functions
                        .entry(self.current.clone())
                        .or_default()
                        .codes
                        .insert(code.to_string());
                }
            }
        }

        fn tokens(&mut self, stream: proc_macro2::TokenStream) {
            for token in stream {
                match token {
                    proc_macro2::TokenTree::Group(group) => self.tokens(group.stream()),
                    proc_macro2::TokenTree::Literal(literal) => {
                        if let Ok(literal) = syn::parse2::<syn::LitStr>(literal.into_token_stream())
                        {
                            self.literal(&literal);
                        }
                    }
                    proc_macro2::TokenTree::Ident(ident) => {
                        // Macro tokens are opaque to syn's expression visitor.
                        // A Response mention conservatively roots the function.
                        if ident == "Response" {
                            self.functions
                                .entry(self.current.clone())
                                .or_default()
                                .wire_boundary = true;
                        }
                        self.functions
                            .entry(self.current.clone())
                            .or_default()
                            .references
                            .insert(ident.to_string());
                    }
                    _ => {}
                }
            }
        }

        fn function(&mut self, signature: &syn::Signature, block: &syn::Block) {
            let name = signature.ident.to_string();
            let previous = std::mem::replace(&mut self.current, name.clone());
            self.functions.entry(name).or_default().wire_boundary |= signature
                .output
                .to_token_stream()
                .to_string()
                .split(|c: char| !c.is_alphanumeric() && c != '_')
                .any(|name| name == "Response");
            self.visit_block(block);
            self.current = previous;
        }

        fn codes(self) -> BTreeSet<String> {
            let mut pending: Vec<_> = self
                .functions
                .iter()
                .filter(|(name, function)| name.is_empty() || function.wire_boundary)
                .map(|(name, _)| name.clone())
                .collect();
            let mut visited = BTreeSet::new();
            let mut codes = BTreeSet::new();
            while let Some(name) = pending.pop() {
                if !visited.insert(name.clone()) {
                    continue;
                }
                if let Some(function) = self.functions.get(&name) {
                    codes.extend(function.codes.iter().cloned());
                    pending.extend(function.references.iter().cloned());
                }
            }
            codes
        }
    }

    impl<'ast> Visit<'ast> for RefusalCensus {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !test_attrs(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !test_attrs(&item.attrs) {
                self.function(&item.sig, &item.block);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !test_attrs(&item.attrs) {
                self.function(&item.sig, &item.block);
            }
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            if let syn::Expr::Path(path) = call.func.as_ref() {
                let names: Vec<_> = path
                    .path
                    .segments
                    .iter()
                    .map(|part| part.ident.to_string())
                    .collect();
                if names.windows(2).any(|pair| pair == ["Response", "err"]) {
                    self.functions
                        .entry(self.current.clone())
                        .or_default()
                        .wire_boundary = true;
                }
                // The typed argument determines this helper's wire code. Its
                // second argument is diagnostic text, not another wire refusal.
                if names.last().is_some_and(|name| name == "refusal")
                    && call.args.first().is_some_and(|arg| matches!(arg, syn::Expr::Path(path)
                        if path.path.segments.iter().any(|part| part.ident == "StatisticalErrorCode")))
                { return; }
            }
            syn::visit::visit_expr_call(self, call);
        }

        fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
            if let Some(name) = path.path.segments.last() {
                self.functions
                    .entry(self.current.clone())
                    .or_default()
                    .references
                    .insert(name.ident.to_string());
            }
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            self.functions
                .entry(self.current.clone())
                .or_default()
                .references
                .insert(call.method.to_string());
            syn::visit::visit_expr_method_call(self, call);
        }

        fn visit_lit_str(&mut self, literal: &'ast syn::LitStr) {
            self.literal(literal);
        }
        fn visit_macro(&mut self, item: &'ast syn::Macro) {
            self.tokens(item.tokens.clone());
        }
    }

    #[test]
    fn refusal_census_keeps_reachable_production_and_excludes_test_and_detail_text() {
        let source = r#"
            fn direct() { Response::err(1, "PRODUCTION_REFUSAL: detail"); }
            fn formatted() -> String { format!("FORMATTED_REFUSAL: {}", 1) }
            fn serve() -> Response { Response::err(1, formatted()) }
            mod ambiguous { fn formatted() -> String { "AMBIGUOUS_HELPER: detail".into() } }
            #[cfg(feature = "security")]
            mod secured { fn serve() -> Response { "SECURITY_REFUSAL: detail"; } }
            #[cfg(test)]
            mod tests { fn planted() -> Response { "UNDECLARED_CODE: backend detail"; } }
            fn after_tests() -> Response { "AFTER_TESTS_REFUSAL: detail"; }
            fn macro_boundary() { wrap!(Response::err(1, "MACRO_REFUSAL: detail")); }
            fn closure_boundary() { let f = || Response::err(1, "CLOSURE_REFUSAL: detail"); }
            fn qualified() -> crate::protocol::Response { "QUALIFIED_REFUSAL: detail"; }
            fn dormant_helper() -> String { "UNSERVED_HELPER: detail".into() }
            fn typed() -> Response {
                refusal(StatisticalErrorCode::ParameterInvalid, format!("DIAGNOSTIC_DETAIL: {}", 1))
            }
        "#;
        let mut census = RefusalCensus::default();
        census.visit_file(&syn::parse_file(source).unwrap());
        assert_eq!(
            census.codes(),
            BTreeSet::from([
                "AMBIGUOUS_HELPER".to_string(),
                "CLOSURE_REFUSAL".to_string(),
                "MACRO_REFUSAL".to_string(),
                "QUALIFIED_REFUSAL".to_string(),
                "AFTER_TESTS_REFUSAL".to_string(),
                "PRODUCTION_REFUSAL".to_string(),
                "FORMATTED_REFUSAL".to_string(),
                "SECURITY_REFUSAL".to_string(),
            ])
        );
    }

    fn assert_catalogued(census: RefusalCensus, codes: &BTreeSet<&str>) {
        for code in census.codes() {
            assert!(
                codes.contains(code.as_str()),
                "undeclared reachable refusal: {code}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "undeclared reachable refusal: NEW_WIRE_REFUSAL")]
    fn refusal_census_rejects_a_new_undeclared_response_code() {
        let mut census = RefusalCensus::default();
        census.visit_file(
            &syn::parse_file(
                r#"fn serve() -> Response {
            Response::err(1, "NEW_WIRE_REFUSAL: detail")
        }"#,
            )
            .unwrap(),
        );
        assert_catalogued(census, &BTreeSet::new());
    }

    // spec: EG-CONTRACT-R014
    #[test]
    fn every_method_error_is_in_the_published_catalog() {
        let catalog: serde_json::Value = serde_json::from_slice(&catalog_json()).unwrap();
        let codes: BTreeSet<_> = catalog["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["code"].as_str().unwrap())
            .collect();
        let mut routed = BTreeSet::new();
        for method in crate::method_descriptors() {
            for code in method_error_set(&method) {
                assert!(codes.contains(code), "{}: {code}", method.id.as_str());
                routed.insert(code);
            }
        }
        assert_eq!(routed, codes, "every published error needs a method route");
        assert!(codes.contains("AUTH_TENANT_MISMATCH"));
        assert!(codes.contains("AUTH_AUDIENCE_MISMATCH"));
        assert!(codes.contains("AUTH_POLICY_VERSION_MISMATCH"));
    }

    #[test]
    fn literal_server_refusals_are_in_the_published_catalog() {
        let catalog: serde_json::Value = serde_json::from_slice(&catalog_json()).unwrap();
        let codes: BTreeSet<_> = catalog["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["code"].as_str().unwrap())
            .collect();
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src/server");
        let mut files = Vec::new();
        server_sources(&root, &mut files);
        assert!(!files.is_empty(), "server source census is empty");
        let mut census = RefusalCensus::default();
        for file in files {
            let source = std::fs::read_to_string(&file).expect("read server source");
            census.visit_file(
                &syn::parse_file(&source)
                    .unwrap_or_else(|error| panic!("{}: {error}", file.display())),
            );
        }
        assert_catalogued(census, &codes);
    }

    #[test]
    fn wire_callable_flags_match_published_and_internal_rows() {
        let catalog = super::super::Catalog::collect();
        let document: serde_json::Value =
            serde_json::from_slice(&super::super::methods_json(&catalog)).unwrap();
        let methods = document["methods"].as_array().unwrap();
        let row = |id| {
            methods
                .iter()
                .find(|method| method["id"].as_str() == Some(id))
                .unwrap()
        };
        assert_eq!(row("Health")["is_wire_callable"], true);
        assert_eq!(row("RegisterUdf")["is_wire_callable"], false);
    }
}
