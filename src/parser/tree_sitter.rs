use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tree_sitter::{Language, Node, Parser};

#[derive(Serialize, Deserialize, Debug)]
pub struct SymbolMetadata {
    pub name: String,
    pub symbol_type: String, // Class, Function, etc.
    pub line: usize,
    pub docstring: Option<String>,
    pub args: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtractedNode {
    pub node_id: String,
    pub node_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtractedEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ParseResult {
    pub nodes: Vec<ExtractedNode>,
    pub edges: Vec<ExtractedEdge>,
    pub symbols_extracted: usize,
}

pub fn parse_file(file_path: &str, source: &[u8]) -> Result<ParseResult, String> {
    let mut parser = Parser::new();
    let language: Language = if file_path.ends_with(".py") {
        tree_sitter_python::LANGUAGE.into()
    } else if file_path.ends_with(".js") {
        tree_sitter_javascript::LANGUAGE.into()
    } else if file_path.ends_with(".ts") {
        tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
    } else if file_path.ends_with(".tsx") {
        tree_sitter_typescript::LANGUAGE_TSX.into()
    } else if file_path.ends_with(".go") {
        tree_sitter_go::LANGUAGE.into()
    } else {
        return Err("Unsupported file extension".into());
    };

    parser
        .set_language(&language)
        .map_err(|e| e.to_string())?;

    let tree = parser
        .parse(source, None)
        .ok_or("Failed to parse source")?;

    let mut result = ParseResult {
        nodes: Vec::new(),
        edges: Vec::new(),
        symbols_extracted: 0,
    };

    let file_node_id = format!("file:{}", file_path);

    walk_node(
        tree.root_node(),
        source,
        file_path,
        &file_node_id,
        &mut result,
    );

    Ok(result)
}

fn walk_node(
    node: Node,
    source: &[u8],
    file_path: &str,
    file_node_id: &str,
    result: &mut ParseResult,
) {
    let kind = node.kind();

    if kind == "class_definition" || kind == "class_declaration" {
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = get_node_text(name_node, source);
            let content_bytes = &source[node.start_byte()..node.end_byte()];
            let mut hasher = Sha256::new();
            hasher.update(content_bytes);
            let content_hash = format!("{:x}", hasher.finalize());
            let symbol_id = format!("symbol:{}", content_hash);

            let mut properties = HashMap::new();
            properties.insert("name".to_string(), name);
            properties.insert("symbol_type".to_string(), "Class".to_string());
            properties.insert("line".to_string(), (node.start_position().row + 1).to_string());
            properties.insert("ast_hash".to_string(), content_hash);
            properties.insert("file_path".to_string(), file_path.to_string());

            result.nodes.push(ExtractedNode {
                node_id: symbol_id.clone(),
                node_type: "SYMBOL".to_string(),
                properties,
            });

            result.edges.push(ExtractedEdge {
                source: file_node_id.to_string(),
                target: symbol_id,
                edge_type: "IMPLEMENTS".to_string(),
                properties: HashMap::new(),
            });

            result.symbols_extracted += 1;
        }
    } else if kind == "function_definition"
        || kind == "function_declaration"
        || kind == "method_definition"
    {
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = get_node_text(name_node, source);
            let content_bytes = &source[node.start_byte()..node.end_byte()];
            let mut hasher = Sha256::new();
            hasher.update(content_bytes);
            let content_hash = format!("{:x}", hasher.finalize());
            let symbol_id = format!("symbol:{}", content_hash);

            let mut properties = HashMap::new();
            properties.insert("name".to_string(), name);
            properties.insert("symbol_type".to_string(), "Function".to_string());
            properties.insert("line".to_string(), (node.start_position().row + 1).to_string());
            properties.insert("ast_hash".to_string(), content_hash);
            properties.insert("file_path".to_string(), file_path.to_string());

            result.nodes.push(ExtractedNode {
                node_id: symbol_id.clone(),
                node_type: "SYMBOL".to_string(),
                properties,
            });

            result.edges.push(ExtractedEdge {
                source: file_node_id.to_string(),
                target: symbol_id,
                edge_type: "IMPLEMENTS".to_string(),
                properties: HashMap::new(),
            });

            result.symbols_extracted += 1;
        }
    } else if kind == "call" {
        if let Some(function_node) = node.child_by_field_name("function") {
            let callee = get_node_text(function_node, source);
            let mut properties = HashMap::new();
            properties.insert("raw".to_string(), callee.clone());
            result.edges.push(ExtractedEdge {
                source: file_node_id.to_string(),
                target: callee,
                edge_type: "calls_raw".to_string(),
                properties,
            });
        }
    } else if kind == "import_statement" || kind == "import_declaration" {
        // Simplified import logic
        let mut properties = HashMap::new();
        properties.insert("raw".to_string(), "import".to_string());
        result.edges.push(ExtractedEdge {
            source: file_node_id.to_string(),
            target: "import_target".to_string(),
            edge_type: "depends_on_raw".to_string(),
            properties,
        });
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        walk_node(child, source, file_path, file_node_id, result);
    }
}

fn get_node_text(node: Node, source: &[u8]) -> String {
    let bytes = &source[node.start_byte()..node.end_byte()];
    String::from_utf8_lossy(bytes).into_owned()
}
