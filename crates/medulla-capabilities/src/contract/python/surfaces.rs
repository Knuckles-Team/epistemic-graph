//! One class per wire shape across the generated package (EH-377).
//!
//! Every hoisted definition is rendered exactly once, by the models renderer, into the
//! module that owns it:
//!
//! * reachable from exactly one DTO surface's roots: that surface's module;
//! * reachable from two or more surfaces: `_shared.py`;
//! * reachable from none: `models.py`.
//!
//! A DTO module re-exports the shared definitions its roots reach, and `models.py`
//! re-exports every definition a DTO module or `_shared.py` owns, so each module keeps
//! its whole public surface while `connector_pack.PackAnnotations` *is*
//! `models.PackAnnotations`. Imports point only from `models` to the surface modules
//! and from a surface module to `_shared`, so the import graph is acyclic and the
//! package still never imports `models` eagerly.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::{Map, Value};

use super::dto::{push_dto_imports, surface_closure, validate_digest_specs};
use super::dto_surfaces::{DtoSurface, DTO_SURFACES};
use super::models::{member_classes, render_owned};
use super::HEADER;

/// The generated module holding the definitions two or more surfaces reach.
pub(super) const SHARED_MODULE: &str = "_shared";
const MODELS_MODULE: &str = "models";

/// Which generated module owns each definition.
pub(super) struct Surfaces<'a> {
    definitions: &'a Map<String, Value>,
    closures: BTreeMap<&'static str, BTreeSet<String>>,
    owners: BTreeMap<String, &'static str>,
    /// Each tagged-union variant class, keyed to the union definition that renders it.
    members: BTreeMap<String, String>,
}

impl<'a> Surfaces<'a> {
    pub(super) fn new(definitions: &'a Map<String, Value>) -> Self {
        let mut closures = BTreeMap::new();
        let mut owners = BTreeMap::new();
        for surface in DTO_SURFACES {
            let Some(names) = surface_closure(surface, definitions) else {
                continue;
            };
            validate_digest_specs(&names, definitions);
            for name in &names {
                owners
                    .entry(name.clone())
                    .and_modify(|owner| *owner = SHARED_MODULE)
                    .or_insert(surface.module);
            }
            closures.insert(surface.module, names);
        }
        let members = definitions
            .iter()
            .flat_map(|(name, node)| {
                member_classes(name, node)
                    .into_iter()
                    .map(move |class| (class, name.clone()))
            })
            .filter(|(class, _)| !definitions.contains_key(class))
            .collect();
        Self {
            definitions,
            closures,
            owners,
            members,
        }
    }

    /// `names` plus the variant classes of every tagged union among them.
    fn with_members(&self, names: &BTreeSet<String>) -> BTreeSet<String> {
        let variants = self
            .members
            .iter()
            .filter(|(_, union)| names.contains(*union))
            .map(|(class, _)| class.clone());
        names.iter().cloned().chain(variants).collect()
    }

    /// The module that renders `name` (a variant class: its union's module).
    fn owner(&self, name: &str) -> &'static str {
        let definition = self.members.get(name).map_or(name, String::as_str);
        self.owners
            .get(definition)
            .copied()
            .unwrap_or(MODELS_MODULE)
    }

    /// Whether `surface`'s roots are declared, so its module is generated.
    pub(super) fn generates(&self, surface: &DtoSurface) -> bool {
        self.closures.contains_key(surface.module)
    }

    /// `models.py`: every definition, rendering those no surface reaches.
    pub(super) fn models_module(&self) -> String {
        let exported = self.definitions.keys().cloned().collect();
        let docstring = "Generated nested engine-contract models (EH-192).";
        self.module(MODELS_MODULE, docstring, &[], &exported)
    }

    /// `_shared.py`: the definitions two or more surfaces reach.
    pub(super) fn shared_module(&self) -> String {
        let exported = self
            .definitions
            .keys()
            .filter(|name| self.owner(name) == SHARED_MODULE)
            .cloned()
            .collect();
        let docstring = "Generated wire DTOs that more than one surface reaches (EH-377).";
        self.module(SHARED_MODULE, docstring, &[], &exported)
    }

    /// One surface's module: its roots and everything they reach.
    pub(super) fn dto_module(&self, surface: &DtoSurface) -> String {
        let docstring = format!("Generated {} nested wire DTOs.", surface.method);
        self.module(
            surface.module,
            &docstring,
            surface.constants,
            &self.closures[surface.module],
        )
    }

    fn module(
        &self,
        module: &str,
        docstring: &str,
        constants: &[(&str, u64)],
        exported: &BTreeSet<String>,
    ) -> String {
        let body = render_owned(self.definitions, &|name| self.owner(name) == module);
        let exported = &self.with_members(exported);
        let mut foreign: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for name in exported {
            let owner = self.owner(name);
            if owner != module {
                foreign.entry(owner).or_default().push(name.clone());
            }
        }
        let mut out = String::from(HEADER);
        let _ = writeln!(out, "\"\"\"{docstring}\"\"\"");
        out.push_str("\nfrom __future__ import annotations\n\n");
        push_dto_imports(&mut out, &body, &foreign);
        if !constants.is_empty() {
            out.push('\n');
        }
        for (name, value) in constants {
            let _ = writeln!(out, "{name} = {value}");
        }
        out.push_str(&body);
        if module != MODELS_MODULE {
            push_rebuilds(&mut out, &body);
        }
        push_all(&mut out, exported);
        out
    }

    /// Every generated module's text, concatenated: the package-wide view the
    /// one-class-per-shape tests read.
    #[cfg(test)]
    pub(super) fn package_text(&self) -> String {
        let mut text = self.models_module();
        text.push_str(&self.shared_module());
        for surface in DTO_SURFACES
            .iter()
            .filter(|surface| self.generates(surface))
        {
            text.push_str(&self.dto_module(surface));
        }
        text
    }
}

/// Complete every class a surface (or `_shared`) module defines at import, as the
/// per-surface modules always did: a caller that introspects `model_fields` (AU's
/// schema-authority gate) sees resolved annotations, not forward references.
/// `models.py` stays lazy (`defer_build`), which is what keeps its import cheap.
fn push_rebuilds(out: &mut String, body: &str) {
    let classes: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("class "))
        .filter_map(|rest| rest.strip_suffix("(BaseModel):"))
        .collect();
    if classes.is_empty() {
        return;
    }
    // Two blank lines after a trailing class (E305); a trailing alias assignment
    // is already followed by one.
    let ends_in_class = body
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty() && !line.starts_with(' '))
        .is_some_and(|line| line.starts_with("class "));
    if ends_in_class {
        out.push('\n');
    }
    for class in classes {
        let _ = writeln!(out, "\n{class}.model_rebuild()");
    }
}

/// The module's public surface, re-exports included (so ruff does not report a
/// re-exported import as unused, F401).
fn push_all(out: &mut String, exported: &BTreeSet<String>) {
    out.push_str("\n\n__all__ = [\n");
    for name in exported {
        let _ = writeln!(out, "    \"{name}\",");
    }
    out.push_str("]\n");
}

#[cfg(test)]
mod tests;
