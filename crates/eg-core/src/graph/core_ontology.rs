use super::*;

impl GraphCore {
    pub fn match_ontology_terms(&self, query: &str) -> Vec<OntologyMatch> {
        if query.trim().is_empty() {
            return Vec::new();
        }
        let current_count = self.node_properties.len();
        {
            let guard = self.ontology_index.read();
            if let Some(idx) = guard.as_ref() {
                if idx.node_count == current_count {
                    return Self::run_ontology_match(&idx.ac, &idx.metas, query);
                }
            }
        }
        let (ac, metas) = self.build_ontology_index();
        let hits = Self::run_ontology_match(&ac, &metas, query);
        *self.ontology_index.write() = Some(OntologyTermIndex {
            node_count: current_count,
            ac,
            metas,
        });
        hits
    }

    /// Scan the node store once, collecting the deduped capability-term → metadata
    /// map and compiling it into an aho-corasick automaton. Terms shorter than 3
    /// chars are dropped (too noise-prone for whole-word matching).
    pub(super) fn build_ontology_index(&self) -> (AhoCorasick, Vec<OntologyMatch>) {
        let mut dedup: HashMap<String, OntologyMatch> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            Self::collect_ontology_terms(&val, &mut dedup);
        }
        Self::compile_ontology_index(dedup)
    }

    /// Fold ONE node's capability terms into the deduped term → metadata map.
    /// A node whose type is not in [`CAPABILITY_NODE_TYPES`] contributes nothing,
    /// and terms shorter than 3 chars are dropped (too noise-prone for whole-word
    /// matching). First writer of a term wins, as before.
    pub(super) fn collect_ontology_terms(
        val: &serde_json::Value,
        dedup: &mut HashMap<String, OntologyMatch>,
    ) {
        let Some(ntype) = val
            .get("type")
            .and_then(|v| v.as_str())
            .or_else(|| val.get("node_type").and_then(|v| v.as_str()))
        else {
            return;
        };
        if !CAPABILITY_NODE_TYPES.contains(&ntype) {
            return;
        }
        let name = val.get("name").and_then(|v| v.as_str()).unwrap_or("");
        // The owning fleet server: a Tool carries `mcp_server`; an MCPServer node
        // IS the server, so fall back to its own name.
        let server = val
            .get("mcp_server")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| if ntype == "MCPServer" { name } else { "" });
        for term in Self::ontology_terms(val, name) {
            let lc = term.to_lowercase();
            if lc.chars().count() < 3 {
                continue;
            }
            dedup.entry(lc).or_insert_with(|| OntologyMatch {
                term: term.to_string(),
                node_type: ntype.to_string(),
                label: name.to_string(),
                mcp_server: server.to_string(),
                score: term.chars().count() as f64,
            });
        }
    }

    /// A node's candidate capability terms: its `name` (when non-empty) followed
    /// by every non-empty string in its `synonyms` array.
    pub(super) fn ontology_terms<'a>(val: &'a serde_json::Value, name: &'a str) -> Vec<&'a str> {
        let mut terms: Vec<&'a str> = Vec::new();
        if !name.is_empty() {
            terms.push(name);
        }
        let Some(arr) = val.get("synonyms").and_then(|v| v.as_array()) else {
            return terms;
        };
        for s in arr {
            let Some(ss) = s.as_str() else {
                continue;
            };
            if !ss.is_empty() {
                terms.push(ss);
            }
        }
        terms
    }

    /// Compile the deduped term map into the aho-corasick automaton plus the
    /// pattern-index-aligned metadata table. A build failure degrades to an empty
    /// automaton (matches nothing) rather than panicking.
    pub(super) fn compile_ontology_index(
        dedup: HashMap<String, OntologyMatch>,
    ) -> (AhoCorasick, Vec<OntologyMatch>) {
        let mut patterns: Vec<String> = Vec::with_capacity(dedup.len());
        let mut metas: Vec<OntologyMatch> = Vec::with_capacity(dedup.len());
        for (lc, meta) in dedup {
            patterns.push(lc);
            metas.push(meta);
        }
        let ac = AhoCorasick::builder()
            .match_kind(MatchKind::LeftmostLongest)
            .build(&patterns)
            .unwrap_or_else(|_| AhoCorasick::new::<[&str; 0], _>([]).expect("empty aho-corasick"));
        (ac, metas)
    }

    /// Run a built automaton over `query`, returning the distinct capability terms
    /// it contains. Matches are restricted to whole words (no alphanumeric char
    /// abutting either end) so a short term never matches inside a larger word.
    pub(super) fn run_ontology_match(
        ac: &AhoCorasick,
        metas: &[OntologyMatch],
        query: &str,
    ) -> Vec<OntologyMatch> {
        if metas.is_empty() {
            return Vec::new();
        }
        let hay = query.to_lowercase();
        let bytes = hay.as_bytes();
        let mut out: Vec<OntologyMatch> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for m in ac.find_iter(hay.as_str()) {
            let start = m.start();
            let end = m.end();
            let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
            let after_ok = end >= bytes.len() || !bytes[end].is_ascii_alphanumeric();
            if !before_ok || !after_ok {
                continue;
            }
            if let Some(meta) = metas.get(m.pattern().as_usize()) {
                if seen.insert(meta.term.to_lowercase()) {
                    out.push(meta.clone());
                }
            }
        }
        out
    }
}
