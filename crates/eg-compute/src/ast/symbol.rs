// CONCEPT:EG-KG.compute.ast-symbol-representation — AST Symbol Representation for KG Persistence
//
// Symbol structs that map directly to Knowledge Graph nodes.
// Each parsed code entity (function, class, import, variable) becomes
// a Symbol with full granularity metadata for ingestion.

use serde::{Deserialize, Serialize};

/// Type of code symbol.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SymbolType {
    Function,
    Class,
    Method,
    Import,
    Variable,
    Module,
    Constant,
    Struct,
    Enum,
    Trait,
    Interface,
    TypeAlias,
}

impl std::fmt::Display for SymbolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            SymbolType::Function => "function",
            SymbolType::Class => "class",
            SymbolType::Method => "method",
            SymbolType::Import => "import",
            SymbolType::Variable => "variable",
            SymbolType::Module => "module",
            SymbolType::Constant => "constant",
            SymbolType::Struct => "struct",
            SymbolType::Enum => "enum",
            SymbolType::Trait => "trait",
            SymbolType::Interface => "interface",
            SymbolType::TypeAlias => "type_alias",
        };
        write!(f, "{}", name)
    }
}

/// A code symbol extracted from AST parsing.
/// Maps directly to a KG `Symbol` node.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Symbol {
    /// Unique ID: "sym:<file_hash>:<name>:<line>"
    pub id: String,
    /// Symbol name (e.g., function name, class name)
    pub name: String,
    /// Fully qualified name including module path
    pub qualified_name: String,
    /// Type of symbol
    pub symbol_type: SymbolType,
    /// Source file path (relative to repository root)
    pub file_path: String,
    /// Start line in source file
    pub line_start: u32,
    /// End line in source file
    pub line_end: u32,
    /// Column offset for the symbol definition
    pub column: u32,
    /// SHA-256 hash of the AST subtree (for change detection)
    pub ast_hash: String,
    /// IDs of other symbols this depends on (imports, calls, inheritance)
    pub dependencies: Vec<String>,
    /// Docstring or comment attached to the symbol
    pub documentation: String,
    /// Language of the source file
    pub language: String,
    /// Whether this symbol is exported (public API)
    pub is_exported: bool,
    /// Decorator/attribute annotations
    pub annotations: Vec<String>,
    /// Byte range in source file (for precise extraction)
    pub byte_start: u32,
    pub byte_end: u32,
}

impl Symbol {
    /// Generate a deterministic ID for this symbol.
    pub fn generate_id(file_path: &str, name: &str, line_start: u32) -> String {
        use sha2::{Digest, Sha256};
        let input = format!("{}:{}:{}", file_path, name, line_start);
        let hash = Sha256::digest(input.as_bytes());
        format!("sym:{}", hex::encode(&hash[..8]))
    }

    /// Convert this symbol to a KG-compatible properties JSON.
    pub fn to_properties_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Result from parsing a file — contains all symbols found.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParseResult {
    pub file_path: String,
    pub language: String,
    pub symbols: Vec<Symbol>,
    pub import_graph: Vec<(String, String)>, // (source_symbol_id, target_module)
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_creation() {
        let sym = Symbol {
            id: Symbol::generate_id("src/main.py", "main", 1),
            name: "main".to_string(),
            qualified_name: "src.main.main".to_string(),
            symbol_type: SymbolType::Function,
            file_path: "src/main.py".to_string(),
            line_start: 1,
            line_end: 10,
            column: 0,
            ast_hash: "abc123".to_string(),
            dependencies: vec![],
            documentation: "Entry point".to_string(),
            language: "python".to_string(),
            is_exported: true,
            annotations: vec!["@click.command()".to_string()],
            byte_start: 0,
            byte_end: 200,
        };
        assert!(sym.id.starts_with("sym:"));
        assert!(!sym.to_properties_json().is_empty());
    }

    #[test]
    fn test_symbol_type_display() {
        assert_eq!(format!("{}", SymbolType::Function), "function");
        assert_eq!(format!("{}", SymbolType::Class), "class");
    }
}
