use super::*;

fn relationship_blob_matches(blob: &[u8], relationship: &str) -> bool {
    decode_property_value(blob)
        .ok()
        .and_then(|value| {
            value
                .get("relationship")
                .and_then(|rel| rel.as_str())
                .map(|value| value == relationship)
        })
        .unwrap_or(false)
}

fn edge_properties_have_relationship(properties: &[Arc<Vec<u8>>], relationship: &str) -> bool {
    properties
        .iter()
        .any(|blob| relationship_blob_matches(blob, relationship))
}

impl GraphCore {
    // ── Scene-graph / 3D world model — one-shot wrappers + queries ────────────
    //     (CONCEPT:EG-KG.compute.scene-graph-primitives)

    /// One-shot [`GraphTxn::add_scene_object`] (CONCEPT:EG-KG.compute.scene-graph-primitives): create the node +
    /// link its parent under ONE write guard, then invalidate the lazy secondary
    /// indexes so a subsequent `scene_children` / `SceneObject` label query sees it.
    /// Returns the new object's id.
    pub fn add_scene_object(&self, pose: &crate::scene::Pose, parent: Option<&str>) -> String {
        let id = self.txn().add_scene_object(pose, parent);
        self.mark_dirty();
        id
    }

    /// One-shot [`GraphTxn::set_pose`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn set_pose(&self, id: &str, pose: &crate::scene::Pose) -> bool {
        let ok = self.txn().set_pose(id, pose);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::reparent`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn reparent(&self, id: &str, new_parent: Option<&str>) -> bool {
        let ok = self.txn().reparent(id, new_parent);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::add_spatial_relation`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn add_spatial_relation(&self, from: &str, to: &str, rel: &str) -> bool {
        let ok = self.txn().add_spatial_relation(from, to, rel);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::set_bounding_volume`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn set_bounding_volume(&self, id: &str, aabb: &crate::scene::Aabb) -> bool {
        let ok = self.txn().set_bounding_volume(id, aabb);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the stored LOCAL pose of scene object `id`. `None` if the
    /// node is absent or carries no (decodable) `pose`.
    pub fn get_pose(&self, id: &str) -> Option<crate::scene::Pose> {
        let blob = self.get_node_properties(id)?;
        let val = decode_property_value(&blob).ok()?;
        crate::scene::Pose::from_json(val.as_object()?.get("pose")?)
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the stored axis-aligned bounding volume of scene object `id`,
    /// in its LOCAL frame. `None` if absent / no (decodable) `aabb`.
    pub fn get_bounding_volume(&self, id: &str) -> Option<crate::scene::Aabb> {
        let blob = self.get_node_properties(id)?;
        let val = decode_property_value(&blob).ok()?;
        crate::scene::Aabb::from_json(val.as_object()?.get("aabb")?)
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the transform parent of scene object `id`: the target of its
    /// outgoing `CHILD_OF` edge, or `None` for a root object / absent node.
    pub fn scene_parent(&self, id: &str) -> Option<String> {
        let idx = *self.topo.read().node_map.get(id)?;
        let targets: Vec<String> = {
            let topo = self.topo.read();
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        targets
            .into_iter()
            .find(|t| self.edge_has_relationship(id, t, "CHILD_OF"))
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the WORLD pose of scene object `id`: its local pose composed
    /// with every ancestor's local pose up the `CHILD_OF` chain (root ∘ … ∘ local).
    /// `None` if `id` is absent / has no pose. A cycle guard bounds the walk (a
    /// well-formed hierarchy is acyclic; the guard just prevents a pathological loop
    /// from spinning). Off-lock pure math once the chain is gathered.
    pub fn world_transform(&self, id: &str) -> Option<crate::scene::Pose> {
        // Gather the chain of ids from `id` up to the root (id first).
        let mut chain: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cur = id.to_string();
        while seen.insert(cur.clone()) {
            chain.push(cur.clone());
            match self.scene_parent(&cur) {
                Some(p) => cur = p,
                None => break,
            }
        }
        // Compose from the ROOT down to `id`: world = root ∘ … ∘ local(id).
        let mut world: Option<crate::scene::Pose> = None;
        for node in chain.iter().rev() {
            let local = self.get_pose(node)?;
            world = Some(match world {
                Some(w) => w.compose(&local),
                None => local,
            });
        }
        world
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the direct transform children of scene object `id`: the
    /// targets of its outgoing `HAS_CHILD` edges. Sorted + deduped; empty if absent /
    /// no children.
    pub fn scene_children(&self, id: &str) -> Vec<String> {
        self.related_targets(id, "HAS_CHILD")
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — all transitive transform descendants of scene object `id`
    /// (breadth-first over `HAS_CHILD`). Sorted + deduped; excludes `id` itself. A
    /// visited-set makes it robust to a malformed cyclic hierarchy.
    pub fn scene_descendants(&self, id: &str) -> Vec<String> {
        let mut out: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        queue.push_back(id.to_string());
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
        visited.insert(id.to_string());
        while let Some(n) = queue.pop_front() {
            for c in self.scene_children(&n) {
                if visited.insert(c.clone()) {
                    out.insert(c.clone());
                    queue.push_back(c);
                }
            }
        }
        out.into_iter().collect()
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — every `(from, to)` pair connected by a spatial relationship
    /// edge carrying `rel` (e.g. `ON`/`IN`/`NEAR`/`SUPPORTS`). Sorted + deduped.
    pub fn objects_with_relation(&self, rel: &str) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .edge_properties
            .iter()
            .filter_map(|entry| {
                let (src, tgt) = entry.key();
                edge_properties_have_relationship(entry.value(), rel)
                    .then(|| (src.clone(), tgt.clone()))
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }
}
