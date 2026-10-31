//! Typed tool steps and their bindings (MEDULLA-DECISION-ENGINE-R134.111.8 to .11).
//!
//! A call is a caller claim. The engine canonicalizes and statically checks it;
//! it never invokes the tool, never infers an edge from a binding and never
//! checks that a referenced output path exists at run time.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{error, valid_id, TaskPlanTask};

pub const MAX_TOOL_STEPS: usize = 64;
pub const MAX_BINDINGS_PER_CALL: usize = 32;
pub const MAX_CALL_ARGUMENT_BYTES: usize = 16 * 1024;
pub const MAX_BINDING_PATH_SEGMENTS: usize = 16;
pub const MAX_BINDING_PATH_BYTES: usize = 256;
pub const BINDING_KEY: &str = "$ref";

/// One deterministic tool call. `arguments` is a JSON object whose values are
/// literals or `{"$ref": "steps.<task_id>.output.<json.path>"}` bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TaskPlanCall {
    pub server: String,
    pub tool: String,
    pub arguments: Value,
}

/// One parsed binding: the source task, its output path and the JSON pointer
/// of the argument that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPlanBinding {
    pub pointer: String,
    pub source_task: String,
    pub reference: String,
}

fn refuse(code: &str, task: &str, pointer: &str) -> String {
    error(code, format!("task={task} pointer={pointer}"))
}

fn pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn valid_key_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Normalize one path segment. A digit run is an array index and loses its
/// leading zeros, so `07` and `7` are the same reference.
fn canonical_segment(segment: &str) -> Option<String> {
    if !segment.is_empty() && segment.bytes().all(|b| b.is_ascii_digit()) {
        let trimmed = segment.trim_start_matches('0');
        return Some(if trimmed.is_empty() { "0" } else { trimmed }.to_string());
    }
    valid_key_segment(segment).then(|| segment.to_string())
}

/// Parse `steps.<task_id>.output[.<path>]` into its task id and canonical text.
fn parse_reference(text: &str) -> Option<(String, String)> {
    let text = text.trim();
    if text.len() > MAX_BINDING_PATH_BYTES {
        return None;
    }
    let rest = text.strip_prefix("steps.")?;
    let (task, path) = match rest.split_once(".output") {
        Some((task, path)) if valid_id(task) && !task.contains('.') => (task, path),
        _ => return None,
    };
    let mut canonical = format!("steps.{task}.output");
    if !path.is_empty() {
        let path = path.strip_prefix('.')?;
        let segments: Vec<&str> = path.split('.').collect();
        if segments.len() > MAX_BINDING_PATH_SEGMENTS {
            return None;
        }
        for segment in segments {
            canonical.push('.');
            canonical.push_str(&canonical_segment(segment)?);
        }
    }
    Some((task.to_string(), canonical))
}

struct Walk<'a> {
    task: &'a str,
    bindings: Vec<TaskPlanBinding>,
}

impl Walk<'_> {
    fn value(&mut self, value: &Value, pointer: &str) -> Result<Value, String> {
        match value {
            Value::Object(map) if map.contains_key(BINDING_KEY) => self.binding(map, pointer),
            Value::Object(map) => self.object(map, pointer),
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, item)| self.value(item, &format!("{pointer}/{i}")))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            literal => Ok(literal.clone()),
        }
    }

    fn object(&mut self, map: &Map<String, Value>, pointer: &str) -> Result<Value, String> {
        let sorted: BTreeMap<&String, &Value> = map.iter().collect();
        let mut out = Map::new();
        for (key, item) in sorted {
            let child = format!("{pointer}/{}", pointer_token(key));
            out.insert(key.clone(), self.value(item, &child)?);
        }
        Ok(Value::Object(out))
    }

    fn binding(&mut self, map: &Map<String, Value>, pointer: &str) -> Result<Value, String> {
        let pointer = if pointer.is_empty() { "/" } else { pointer };
        let parsed = match (map.len(), &map[BINDING_KEY]) {
            (1, Value::String(text)) => parse_reference(text),
            _ => None,
        };
        let Some((source_task, reference)) = parsed else {
            return Err(refuse("binding_bad_path", self.task, pointer));
        };
        self.bindings.push(TaskPlanBinding {
            pointer: pointer.to_string(),
            source_task,
            reference: reference.clone(),
        });
        let mut out = Map::new();
        out.insert(BINDING_KEY.to_string(), Value::String(reference));
        Ok(Value::Object(out))
    }
}

/// Canonicalize one call: sorted keys, no whitespace, one reference form.
/// Returns the canonical call, its bindings and the call's text byte count.
pub(super) fn canonical_call(
    task: &str,
    call: &TaskPlanCall,
) -> Result<(TaskPlanCall, Vec<TaskPlanBinding>, usize), String> {
    let object = call.arguments.as_object();
    if !valid_id(&call.server)
        || !valid_id(&call.tool)
        || object.is_none_or(|map| map.contains_key(BINDING_KEY))
    {
        return Err(refuse("call_invalid", task, ""));
    }
    let mut walk = Walk {
        task,
        bindings: Vec::new(),
    };
    let arguments = walk.value(&call.arguments, "")?;
    let argument_bytes = serde_json::to_vec(&arguments)
        .map_err(|e| error("call_invalid", e))?
        .len();
    if argument_bytes > MAX_CALL_ARGUMENT_BYTES {
        return Err(refuse("call_bounds", task, ""));
    }
    if walk.bindings.len() > MAX_BINDINGS_PER_CALL {
        return Err(refuse("call_bounds", task, &walk.bindings[0].pointer));
    }
    let bytes = call.server.len() + call.tool.len() + argument_bytes;
    let canonical = TaskPlanCall {
        server: call.server.clone(),
        tool: call.tool.clone(),
        arguments,
    };
    Ok((canonical, walk.bindings, bytes))
}

/// The bindings of a canonical task, in pointer order.
pub fn task_bindings(task: &TaskPlanTask) -> Vec<TaskPlanBinding> {
    let Some(call) = &task.call else {
        return Vec::new();
    };
    canonical_call(&task.task_id, call)
        .map(|(_, bindings, _)| bindings)
        .unwrap_or_default()
}

fn ancestors(tasks: &BTreeMap<String, TaskPlanTask>, id: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![id.to_string()];
    while let Some(next) = pending.pop() {
        for dependency in tasks[&next].depends_on.iter() {
            if seen.insert(dependency.clone()) {
                pending.push(dependency.clone());
            }
        }
    }
    seen
}

/// Check every binding against the acyclic task graph. A binding adds no
/// edge; it may only read a direct dependency's output.
pub(super) fn check_bindings(tasks: &BTreeMap<String, TaskPlanTask>) -> Result<(), String> {
    let tool_steps = tasks.values().filter(|t| t.call.is_some()).count();
    if tool_steps > MAX_TOOL_STEPS {
        return Err(error("call_bounds", "too many tool steps"));
    }
    for task in tasks.values() {
        for binding in task_bindings(task) {
            let (id, pointer) = (task.task_id.as_str(), binding.pointer.as_str());
            if !tasks.contains_key(&binding.source_task) {
                return Err(refuse("binding_unknown_task", id, pointer));
            }
            if binding.source_task == id || ancestors(tasks, &binding.source_task).contains(id) {
                return Err(refuse("binding_cycle", id, pointer));
            }
            if !task.depends_on.iter().any(|d| *d == binding.source_task) {
                return Err(refuse("binding_not_dependency", id, pointer));
            }
        }
    }
    Ok(())
}
