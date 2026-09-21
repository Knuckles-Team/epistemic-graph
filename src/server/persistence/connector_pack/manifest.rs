//! Conservative extraction of ingestion mappings from an SDK manifest body.
//!
//! Connector manifests are SDK-validated YAML. EG persists only the closed
//! mapping projection ingestion consumes; it does not reinterpret the rest of
//! the document. This parser accepts the block-map representation emitted by
//! the SDK and fails closed on YAML features inside `schema_mappings` that
//! would require implicit typing, aliases or tag processing.

use std::collections::BTreeMap;

use eg_types::connector_pack::ConnectorSchemaMapping;

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
}
