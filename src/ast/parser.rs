// CONCEPT:KG-2.21a — Multi-Language AST Parser
//
// Tree-sitter based parser that extracts full-granularity Symbol nodes
// from Python, Rust, TypeScript, JavaScript, and Go source files.
// Each extracted symbol is emitted to the graph mutation ledger.

use super::symbol::{ParseResult, Symbol, SymbolType};
use tree_sitter::{Language, Node, Parser};

/// Supported languages for AST parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceLanguage {
    Python,
    Rust,
    TypeScript,
    JavaScript,
    Go,
}

impl SourceLanguage {
    /// Detect language from file extension.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "py" => Some(SourceLanguage::Python),
            "rs" => Some(SourceLanguage::Rust),
            "ts" | "tsx" => Some(SourceLanguage::TypeScript),
            "js" | "jsx" | "mjs" => Some(SourceLanguage::JavaScript),
            "go" => Some(SourceLanguage::Go),
            _ => None,
        }
    }

    /// Get the tree-sitter language for this source language.
    fn tree_sitter_language(&self) -> Language {
        unsafe {
            match self {
                SourceLanguage::Python => tree_sitter_python(),
                SourceLanguage::Rust => tree_sitter_rust(),
                SourceLanguage::TypeScript => tree_sitter_typescript(),
                SourceLanguage::JavaScript => tree_sitter_javascript(),
                SourceLanguage::Go => tree_sitter_go(),
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            SourceLanguage::Python => "python",
            SourceLanguage::Rust => "rust",
            SourceLanguage::TypeScript => "typescript",
            SourceLanguage::JavaScript => "javascript",
            SourceLanguage::Go => "go",
        }
    }
}

/// Parse a source file and extract all symbols.
pub fn parse_file(source_code: &str, file_path: &str, language: SourceLanguage) -> ParseResult {
    let mut parser = Parser::new();
    let ts_lang = language.tree_sitter_language();
    if parser.set_language(&ts_lang).is_err() {
        return ParseResult {
            file_path: file_path.to_string(),
            language: language.name().to_string(),
            symbols: vec![],
            import_graph: vec![],
            errors: vec!["Failed to set tree-sitter language".to_string()],
        };
    }

    let tree = match parser.parse(source_code, None) {
        Some(t) => t,
        None => {
            return ParseResult {
                file_path: file_path.to_string(),
                language: language.name().to_string(),
                symbols: vec![],
                import_graph: vec![],
                errors: vec!["Failed to parse source code".to_string()],
            }
        }
    };

    let mut symbols = Vec::new();
    let mut import_graph = Vec::new();
    let mut errors = Vec::new();

    let root = tree.root_node();
    extract_symbols(
        &root,
        source_code,
        file_path,
        language,
        &mut symbols,
        &mut import_graph,
        &mut errors,
    );

    ParseResult {
        file_path: file_path.to_string(),
        language: language.name().to_string(),
        symbols,
        import_graph,
        errors,
    }
}

/// Recursively extract symbols from the AST.
fn extract_symbols(
    node: &Node,
    source: &str,
    file_path: &str,
    language: SourceLanguage,
    symbols: &mut Vec<Symbol>,
    import_graph: &mut Vec<(String, String)>,
    errors: &mut Vec<String>,
) {
    let kind = node.kind();

    match language {
        SourceLanguage::Python => {
            extract_python_symbol(node, source, file_path, kind, symbols, import_graph)
        }
        SourceLanguage::Rust => {
            extract_rust_symbol(node, source, file_path, kind, symbols, import_graph)
        }
        SourceLanguage::TypeScript | SourceLanguage::JavaScript => {
            extract_js_ts_symbol(
                node,
                source,
                file_path,
                kind,
                language,
                symbols,
                import_graph,
            );
        }
        SourceLanguage::Go => {
            extract_go_symbol(node, source, file_path, kind, symbols, import_graph)
        }
    }

    // Recurse into children
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        extract_symbols(
            &child,
            source,
            file_path,
            language,
            symbols,
            import_graph,
            errors,
        );
    }
}

/// Extract Python-specific symbols.
fn extract_python_symbol(
    node: &Node,
    source: &str,
    file_path: &str,
    kind: &str,
    symbols: &mut Vec<Symbol>,
    import_graph: &mut Vec<(String, String)>,
) {
    let (symbol_type, name_node_kind) = match kind {
        "function_definition" => (SymbolType::Function, "name"),
        "class_definition" => (SymbolType::Class, "name"),
        "import_statement" | "import_from_statement" => {
            let text = node_text(node, source);
            let id = Symbol::generate_id(file_path, &text, node.start_position().row as u32 + 1);
            symbols.push(Symbol {
                id: id.clone(),
                name: text.clone(),
                qualified_name: text.clone(),
                symbol_type: SymbolType::Import,
                file_path: file_path.to_string(),
                line_start: node.start_position().row as u32 + 1,
                line_end: node.end_position().row as u32 + 1,
                column: node.start_position().column as u32,
                ast_hash: compute_ast_hash(node, source),
                dependencies: vec![],
                documentation: String::new(),
                language: "python".to_string(),
                is_exported: false,
                annotations: vec![],
                byte_start: node.start_byte() as u32,
                byte_end: node.end_byte() as u32,
            });
            import_graph.push((id, text));
            return;
        }
        _ => return,
    };

    if let Some(name_node) = node.child_by_field_name(name_node_kind) {
        let name = node_text(&name_node, source);
        let id = Symbol::generate_id(file_path, &name, node.start_position().row as u32 + 1);

        // Extract docstring
        let doc = extract_python_docstring(node, source);

        // Extract decorators
        let annotations = extract_python_decorators(node, source);

        symbols.push(Symbol {
            id,
            name: name.clone(),
            qualified_name: format!(
                "{}.{}",
                file_path.replace('/', ".").trim_end_matches(".py"),
                name
            ),
            symbol_type,
            file_path: file_path.to_string(),
            line_start: node.start_position().row as u32 + 1,
            line_end: node.end_position().row as u32 + 1,
            column: node.start_position().column as u32,
            ast_hash: compute_ast_hash(node, source),
            dependencies: vec![],
            documentation: doc,
            language: "python".to_string(),
            is_exported: !name.starts_with('_'),
            annotations,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }
}

/// Extract Rust-specific symbols.
fn extract_rust_symbol(
    node: &Node,
    source: &str,
    file_path: &str,
    kind: &str,
    symbols: &mut Vec<Symbol>,
    _import_graph: &mut Vec<(String, String)>,
) {
    let (symbol_type, name_node_kind) = match kind {
        "function_item" => (SymbolType::Function, "name"),
        "struct_item" => (SymbolType::Struct, "name"),
        "enum_item" => (SymbolType::Enum, "name"),
        "trait_item" => (SymbolType::Trait, "name"),
        "type_item" => (SymbolType::TypeAlias, "name"),
        "impl_item" => return, // Methods handled via function_item children
        "const_item" | "static_item" => (SymbolType::Constant, "name"),
        _ => return,
    };

    if let Some(name_node) = node.child_by_field_name(name_node_kind) {
        let name = node_text(&name_node, source);
        let id = Symbol::generate_id(file_path, &name, node.start_position().row as u32 + 1);

        // Check for `pub` visibility
        let is_exported = node_text(node, source).starts_with("pub ");

        symbols.push(Symbol {
            id,
            name: name.clone(),
            qualified_name: format!(
                "{}::{}",
                file_path.replace('/', "::").trim_end_matches(".rs"),
                name
            ),
            symbol_type,
            file_path: file_path.to_string(),
            line_start: node.start_position().row as u32 + 1,
            line_end: node.end_position().row as u32 + 1,
            column: node.start_position().column as u32,
            ast_hash: compute_ast_hash(node, source),
            dependencies: vec![],
            documentation: String::new(),
            language: "rust".to_string(),
            is_exported,
            annotations: vec![],
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }
}

/// Extract JS/TS-specific symbols.
fn extract_js_ts_symbol(
    node: &Node,
    source: &str,
    file_path: &str,
    kind: &str,
    language: SourceLanguage,
    symbols: &mut Vec<Symbol>,
    _import_graph: &mut Vec<(String, String)>,
) {
    let (symbol_type, name_node_kind) = match kind {
        "function_declaration" => (SymbolType::Function, "name"),
        "class_declaration" => (SymbolType::Class, "name"),
        "interface_declaration" => (SymbolType::Interface, "name"),
        "type_alias_declaration" => (SymbolType::TypeAlias, "name"),
        "enum_declaration" => (SymbolType::Enum, "name"),
        _ => return,
    };

    if let Some(name_node) = node.child_by_field_name(name_node_kind) {
        let name = node_text(&name_node, source);
        let id = Symbol::generate_id(file_path, &name, node.start_position().row as u32 + 1);

        let is_exported = {
            if let Some(parent) = node.parent() {
                parent.kind() == "export_statement"
            } else {
                false
            }
        };

        symbols.push(Symbol {
            id,
            name: name.clone(),
            qualified_name: name.clone(),
            symbol_type,
            file_path: file_path.to_string(),
            line_start: node.start_position().row as u32 + 1,
            line_end: node.end_position().row as u32 + 1,
            column: node.start_position().column as u32,
            ast_hash: compute_ast_hash(node, source),
            dependencies: vec![],
            documentation: String::new(),
            language: language.name().to_string(),
            is_exported,
            annotations: vec![],
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }
}

/// Extract Go-specific symbols.
fn extract_go_symbol(
    node: &Node,
    source: &str,
    file_path: &str,
    kind: &str,
    symbols: &mut Vec<Symbol>,
    _import_graph: &mut Vec<(String, String)>,
) {
    let (symbol_type, name_node_kind) = match kind {
        "function_declaration" => (SymbolType::Function, "name"),
        "method_declaration" => (SymbolType::Method, "name"),
        "type_declaration" => (SymbolType::Struct, "name"), // Simplified
        _ => return,
    };

    if let Some(name_node) = node.child_by_field_name(name_node_kind) {
        let name = node_text(&name_node, source);
        let id = Symbol::generate_id(file_path, &name, node.start_position().row as u32 + 1);

        // Go: exported if first char is uppercase
        let is_exported = name.chars().next().map_or(false, |c| c.is_uppercase());

        symbols.push(Symbol {
            id,
            name: name.clone(),
            qualified_name: name.clone(),
            symbol_type,
            file_path: file_path.to_string(),
            line_start: node.start_position().row as u32 + 1,
            line_end: node.end_position().row as u32 + 1,
            column: node.start_position().column as u32,
            ast_hash: compute_ast_hash(node, source),
            dependencies: vec![],
            documentation: String::new(),
            language: "go".to_string(),
            is_exported,
            annotations: vec![],
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn node_text<'a>(node: &Node<'a>, source: &'a str) -> String {
    source[node.start_byte()..node.end_byte()].to_string()
}

fn compute_ast_hash(node: &Node, source: &str) -> String {
    use sha2::{Digest, Sha256};
    let text = &source[node.start_byte()..node.end_byte()];
    let hash = Sha256::digest(text.as_bytes());
    hex::encode(&hash[..16])
}

fn extract_python_docstring(node: &Node, source: &str) -> String {
    // Look for expression_statement > string as first child of body
    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "expression_statement" {
                let mut inner_cursor = child.walk();
                for inner in child.children(&mut inner_cursor) {
                    if inner.kind() == "string" {
                        let text = node_text(&inner, source);
                        return text.trim_matches('"').trim_matches('\'').to_string();
                    }
                }
            }
            break; // Only check first statement
        }
    }
    String::new()
}

fn extract_python_decorators(node: &Node, source: &str) -> Vec<String> {
    let mut decorators = Vec::new();
    if let Some(parent) = node.parent() {
        // Decorators are siblings before the function/class node
        let mut cursor = parent.walk();
        for child in parent.children(&mut cursor) {
            if child.kind() == "decorator" {
                decorators.push(node_text(&child, source));
            }
            if child.id() == node.id() {
                break;
            }
        }
    }
    decorators
}

// ── FFI declarations ─────────────────────────────────────────────────────

extern "C" {
    fn tree_sitter_python() -> Language;
    fn tree_sitter_rust() -> Language;
    fn tree_sitter_typescript() -> Language;
    fn tree_sitter_javascript() -> Language;
    fn tree_sitter_go() -> Language;
}
