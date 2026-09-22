//! Conservative extraction of ingestion mappings from an SDK manifest body.
//!
//! Connector manifests are SDK-validated YAML. EG persists only the closed
//! mapping projection ingestion consumes; it does not reinterpret the rest of
//! the document. This parser accepts the block-map representation emitted by
//! the SDK and fails closed on YAML features inside `schema_mappings` that
//! would require implicit typing, aliases or tag processing.

use std::collections::BTreeMap;

use eg_types::connector_pack::{ConnectorRelationshipMapping, ConnectorSchemaMapping};

pub fn decode_schema_mappings(
    body: &[u8],
) -> Result<BTreeMap<String, ConnectorSchemaMapping>, String> {
    let text =
        std::str::from_utf8(body).map_err(|_| "connector manifest is not UTF-8".to_string())?;
    if text.starts_with('\u{feff}') || text.contains('\t') {
        return Err("connector manifest uses unsupported YAML whitespace".to_string());
    }
    let lines: Vec<&str> = text.lines().collect();
    let Some(start) = lines
        .iter()
        .position(|line| strip_comment(line).trim_end() == "schema_mappings:")
    else {
        return Ok(BTreeMap::new());
    };
    parse_mapping_block(&lines[start + 1..])
}

/// Extract the closed `resources[*].relations[*]` projection used by native
/// ingestion. Keys are `<source-resource>/<relationship>` and therefore map
/// losslessly to the exact manifest reference path.
pub fn decode_relationship_mappings(
    body: &[u8],
) -> Result<BTreeMap<String, ConnectorRelationshipMapping>, String> {
    let text =
        std::str::from_utf8(body).map_err(|_| "connector manifest is not UTF-8".to_string())?;
    if text.starts_with('\u{feff}') || text.contains('\t') {
        return Err("connector manifest uses unsupported YAML whitespace".to_string());
    }
    let mut parser = RelationshipParser::default();
    for raw in text.lines() {
        let line = strip_comment(raw).trim_end();
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        if !parser.consume(indent, line.trim())? {
            break;
        }
    }
    parser.finish()
}

#[derive(Default)]
struct RelationshipParser {
    mappings: BTreeMap<String, ConnectorRelationshipMapping>,
    in_resources: bool,
    source_resource: Option<String>,
    relation: Option<ConnectorRelationshipMapping>,
}

impl RelationshipParser {
    fn consume(&mut self, indent: usize, line: &str) -> Result<bool, String> {
        if !self.in_resources {
            return Ok(self.seek_resources(indent, line));
        }
        self.consume_resource_line(indent, line)
    }

    fn seek_resources(&mut self, indent: usize, line: &str) -> bool {
        self.in_resources = indent == 0 && line == "resources:";
        true
    }

    fn consume_resource_line(&mut self, indent: usize, line: &str) -> Result<bool, String> {
        if indent == 0 && !line.starts_with("- name:") {
            finish_relation(&mut self.mappings, self.relation.take())?;
            return Ok(false);
        }
        match indent {
            0 if line.starts_with("- name:") => self.start_resource(line)?,
            2 if line.starts_with("- name:") => self.start_relation(line)?,
            4 if self.relation.is_some() => self.set_relation_property(line)?,
            _ => {}
        }
        Ok(true)
    }

    fn start_resource(&mut self, line: &str) -> Result<(), String> {
        finish_relation(&mut self.mappings, self.relation.take())?;
        self.source_resource = Some(yaml_scalar(line.trim_start_matches("- name:").trim())?);
        Ok(())
    }

    fn start_relation(&mut self, line: &str) -> Result<(), String> {
        finish_relation(&mut self.mappings, self.relation.take())?;
        let source = self
            .source_resource
            .clone()
            .ok_or_else(|| "connector relation has no source resource".to_string())?;
        self.relation = Some(ConnectorRelationshipMapping {
            source_resource: source,
            relationship: yaml_scalar(line.trim_start_matches("- name:").trim())?,
            target_resource: String::new(),
            lpg_rel_type: String::new(),
        });
        Ok(())
    }

    fn set_relation_property(&mut self, line: &str) -> Result<(), String> {
        let active = self
            .relation
            .as_mut()
            .ok_or_else(|| "connector relation property has no relation".to_string())?;
        let (key, value) = split_yaml_pair(line)?;
        match key {
            "target" => active.target_resource = yaml_scalar(value)?,
            "lpg_rel_type" => active.lpg_rel_type = nullable_scalar(value)?,
            "label" => {}
            _ => return Err("connector relation contains an unsupported field".to_string()),
        }
        Ok(())
    }

    fn finish(mut self) -> Result<BTreeMap<String, ConnectorRelationshipMapping>, String> {
        finish_relation(&mut self.mappings, self.relation)?;
        Ok(self.mappings)
    }
}

fn finish_relation(
    mappings: &mut BTreeMap<String, ConnectorRelationshipMapping>,
    relation: Option<ConnectorRelationshipMapping>,
) -> Result<(), String> {
    let Some(relation) = relation else {
        return Ok(());
    };
    if relation.source_resource.is_empty()
        || relation.relationship.is_empty()
        || relation.target_resource.is_empty()
    {
        return Err("connector relation is missing source, name, or target".to_string());
    }
    let key = format!("{}/{}", relation.source_resource, relation.relationship);
    if mappings.insert(key, relation).is_some() {
        return Err("connector relations contain a duplicate source/name".to_string());
    }
    Ok(())
}

fn parse_mapping_block(lines: &[&str]) -> Result<BTreeMap<String, ConnectorSchemaMapping>, String> {
    let mut parser = MappingParser::default();
    for raw in lines {
        let line = strip_comment(raw).trim_end();
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        if indent == 0 {
            break;
        }
        parser.consume(indent, line.trim())?;
    }
    parser.finish()
}

#[derive(Default)]
struct MappingParser {
    mappings: BTreeMap<String, ConnectorSchemaMapping>,
    current: Option<(String, ConnectorSchemaMapping)>,
    in_fields: bool,
}

impl MappingParser {
    fn consume(&mut self, indent: usize, line: &str) -> Result<(), String> {
        match indent {
            2 => self.start_mapping(line),
            4 => self.set_mapping_property(line),
            6 if self.in_fields => self.insert_field(line),
            _ => Err("schema mapping YAML indentation is unsupported".to_string()),
        }
    }

    fn start_mapping(&mut self, line: &str) -> Result<(), String> {
        finish_mapping(&mut self.mappings, self.current.take())?;
        let (key, value) = split_yaml_pair(line)?;
        if !value.is_empty() {
            return Err("schema mapping must use a YAML block map".to_string());
        }
        self.current = Some((yaml_scalar(key)?, empty_mapping()));
        self.in_fields = false;
        Ok(())
    }

    fn set_mapping_property(&mut self, line: &str) -> Result<(), String> {
        let (_, mapping) = self
            .current
            .as_mut()
            .ok_or_else(|| "schema mapping field has no mapping key".to_string())?;
        let (key, value) = split_yaml_pair(line)?;
        match key {
            "ontology_class" => {
                mapping.ontology_class = nullable_scalar(value)?;
                self.in_fields = false;
                Ok(())
            }
            "fields" if value.is_empty() => {
                self.in_fields = true;
                Ok(())
            }
            "fields" if value == "{}" => {
                self.in_fields = false;
                Ok(())
            }
            _ => Err("schema mapping contains an unsupported field".to_string()),
        }
    }

    fn insert_field(&mut self, line: &str) -> Result<(), String> {
        let (_, mapping) = self
            .current
            .as_mut()
            .ok_or_else(|| "schema mapping field has no mapping key".to_string())?;
        let (source, target) = split_yaml_pair(line)?;
        let source = yaml_scalar(source)?;
        let target = yaml_scalar(target)?;
        if source.is_empty() || target.is_empty() || mapping.fields.insert(source, target).is_some()
        {
            return Err("schema mapping fields contain an empty or duplicate key".to_string());
        }
        Ok(())
    }

    fn finish(mut self) -> Result<BTreeMap<String, ConnectorSchemaMapping>, String> {
        finish_mapping(&mut self.mappings, self.current)?;
        Ok(self.mappings)
    }
}

fn empty_mapping() -> ConnectorSchemaMapping {
    ConnectorSchemaMapping {
        ontology_class: String::new(),
        fields: BTreeMap::new(),
    }
}

fn finish_mapping(
    mappings: &mut BTreeMap<String, ConnectorSchemaMapping>,
    current: Option<(String, ConnectorSchemaMapping)>,
) -> Result<(), String> {
    if let Some((key, mapping)) = current {
        if key.is_empty() || mappings.insert(key, mapping).is_some() {
            return Err("schema mappings contain an empty or duplicate key".to_string());
        }
    }
    Ok(())
}

fn split_yaml_pair(line: &str) -> Result<(&str, &str), String> {
    let index = unquoted_character(line, ':')
        .ok_or_else(|| "schema mapping YAML entry has no key separator".to_string())?;
    Ok((line[..index].trim(), line[index + 1..].trim()))
}

fn unquoted_character(line: &str, wanted: char) -> Option<usize> {
    let mut state = YamlQuoteState::default();
    for (index, character) in line.char_indices() {
        if state.is_unquoted(character, wanted) {
            return Some(index);
        }
        state.advance(character);
    }
    None
}

#[derive(Default)]
struct YamlQuoteState {
    quote: Option<char>,
    escaped: bool,
}

impl YamlQuoteState {
    fn is_unquoted(&self, character: char, wanted: char) -> bool {
        character == wanted && self.quote.is_none()
    }

    fn advance(&mut self, character: char) {
        if self.quote == Some('"') && character == '\\' && !self.escaped {
            self.escaped = true;
            return;
        }
        if matches!(character, '\'' | '"') && !self.escaped {
            self.quote = match self.quote {
                Some(active) if active == character => None,
                None => Some(character),
                active => active,
            };
        }
        self.escaped = false;
    }
}

fn nullable_scalar(value: &str) -> Result<String, String> {
    if matches!(value, "" | "null" | "Null" | "NULL" | "~") {
        Ok(String::new())
    } else {
        yaml_scalar(value)
    }
}

fn yaml_scalar(value: &str) -> Result<String, String> {
    if value.is_empty()
        || value
            .chars()
            .next()
            .is_some_and(|first| matches!(first, '&' | '*' | '!' | '[' | '{' | '|' | '>'))
    {
        return Err("schema mapping uses an unsupported YAML value".to_string());
    }
    if value.starts_with('"') {
        return serde_json::from_str::<String>(value)
            .map_err(|_| "schema mapping has an invalid quoted value".to_string());
    }
    if value.starts_with('\'') {
        return value
            .strip_suffix('\'')
            .map(|inner| inner.replace("''", "'"))
            .ok_or_else(|| "schema mapping has an invalid quoted value".to_string());
    }
    Ok(value.to_string())
}

fn strip_comment(line: &str) -> &str {
    unquoted_character(line, '#').map_or(line, |index| &line[..index])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_sdk_block_schema_mappings() {
        let mappings = decode_schema_mappings(
            b"connector: demo\nschema_mappings:\n  DemoItem:\n    ontology_class: Document\n    fields:\n      id: identifier\nprovenance:\n  generated_by: test\n",
        )
        .unwrap();
        assert_eq!(mappings["DemoItem"].ontology_class, "Document");
        assert_eq!(mappings["DemoItem"].fields["id"], "identifier");
    }

    #[test]
    fn refuses_aliases_inside_mapping_projection() {
        assert!(decode_schema_mappings(
            b"schema_mappings:\n  DemoItem:\n    ontology_class: *class\n"
        )
        .is_err());
    }

    #[test]
    fn extracts_exact_resource_relationship_paths() {
        let relations = decode_relationship_mappings(
            b"connector: demo\nresources:\n- name: Document\n  relations:\n  - name: contains\n    label: contains\n    target: Section\n    lpg_rel_type: CONTAINS\nschema_mappings: {}\n",
        )
        .unwrap();
        assert_eq!(relations["Document/contains"].source_resource, "Document");
        assert_eq!(relations["Document/contains"].target_resource, "Section");
        assert_eq!(relations["Document/contains"].lpg_rel_type, "CONTAINS");
    }
}
