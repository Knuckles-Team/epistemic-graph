use super::*;

impl<'a> GraphTxn<'a> {
    // ── Scene-graph / 3D world model (CONCEPT:EG-KG.compute.scene-graph-primitives) ─────────────────────────
    //
    // A `:SceneObject` node carries a local `pose` (translation + rotation quaternion
    // + scale) in its property blob; a parent/child transform hierarchy is expressed
    // with reciprocal typed edges — child →`CHILD_OF`→ parent and parent →`HAS_CHILD`
    // → child — so the world transform is the composition of local poses up the
    // parent chain. Optional spatial relations (`ON`/`IN`/`NEAR`/`SUPPORTS`) and an
    // axis-aligned bounding volume attach with the same edge/property conventions the
    // memory primitives use. Deterministic (ids derived from graph state + inputs, no
    // clock/RNG) and atomic under the held write guard, so it replays identically.

    /// Deterministic id for a new scene object over `(sequence, parent, pose)`
    /// (CONCEPT:EG-KG.compute.scene-graph-primitives). `sequence` is the live node count at insertion time, which
    /// is monotonic under replay, so identical WAL replay yields the identical id
    /// while distinct inserts never collide. No RNG/clock.
    pub(super) fn derive_scene_id(
        sequence: usize,
        parent: Option<&str>,
        pose: &crate::scene::Pose,
    ) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sequence.hash(&mut h);
        parent.unwrap_or("").hash(&mut h);
        // Hash the canonical pose JSON string so the seed depends on the pose too.
        pose.to_json().to_string().hash(&mut h);
        format!("scene:{:016x}", h.finish())
    }

    /// Write the reciprocal parent/child transform edges (child `CHILD_OF` parent +
    /// parent `HAS_CHILD` child) idempotently, skipping an absent parent so no
    /// dangling edge is created. Runs under the held write guard.
    pub(super) fn link_parent(&mut self, child: &str, parent: &str) {
        if child == parent || !self.topo.node_map.contains_key(parent) {
            return;
        }
        if !self.has_relationship_edge(child, parent, "CHILD_OF") {
            if let Ok(e) = rmp_serde::to_vec_named(&serde_json::json!({"relationship": "CHILD_OF"}))
            {
                let _ = self.add_edge(child.to_string(), parent.to_string(), e);
            }
        }
        if !self.has_relationship_edge(parent, child, "HAS_CHILD") {
            if let Ok(e) =
                rmp_serde::to_vec_named(&serde_json::json!({"relationship": "HAS_CHILD"}))
            {
                let _ = self.add_edge(parent.to_string(), child.to_string(), e);
            }
        }
    }

    /// The current transform parent of `child`: the target of its outgoing `CHILD_OF`
    /// edge, if any. Read under the held write guard.
    pub(super) fn transform_parent(&self, child: &str) -> Option<String> {
        let &idx = self.topo.node_map.get(child)?;
        let targets: Vec<String> = self
            .topo
            .graph
            .edges_directed(idx, petgraph::Direction::Outgoing)
            .map(|e| self.topo.graph[e.target()].clone())
            .collect();
        targets
            .into_iter()
            .find(|t| self.has_relationship_edge(child, t, "CHILD_OF"))
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — create a `:SceneObject` node with local `pose`, optionally
    /// parented under `parent` via reciprocal `CHILD_OF`/`HAS_CHILD` edges (an absent
    /// parent is skipped — no dangling edge). Returns the new object's deterministic
    /// id. Runs under the held write guard.
    pub fn add_scene_object(&mut self, pose: &crate::scene::Pose, parent: Option<&str>) -> String {
        let id = Self::derive_scene_id(self.topo.node_map.len(), parent, pose);
        let obj = serde_json::json!({ "type": "SceneObject", "pose": pose.to_json() });
        if let Ok(blob) = rmp_serde::to_vec_named(&obj) {
            self.add_node(id.clone(), blob);
        }
        if let Some(p) = parent {
            self.link_parent(&id, p);
        }
        id
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — replace the local `pose` of scene object `id`. No-op
    /// returning `false` if the node is absent. Runs under the held write guard.
    pub fn set_pose(&mut self, id: &str, pose: &crate::scene::Pose) -> bool {
        let mut update = serde_json::Map::new();
        update.insert("pose".to_string(), pose.to_json());
        self.merge_fields(id, &update)
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — reparent scene object `id` under `new_parent` (or detach to a
    /// root with `None`): drops the existing reciprocal transform edges to the old
    /// parent and links the new one, so `world_transform` recomposes down the new
    /// chain. No-op returning `false` if `id` is absent or `new_parent` is missing.
    /// Runs under the held write guard.
    pub fn reparent(&mut self, id: &str, new_parent: Option<&str>) -> bool {
        if !self.topo.node_map.contains_key(id) {
            return false;
        }
        if let Some(np) = new_parent {
            if np == id || !self.topo.node_map.contains_key(np) {
                return false;
            }
        }
        if let Some(old) = self.transform_parent(id) {
            self.remove_edge(id.to_string(), old.clone());
            self.remove_edge(old, id.to_string());
        }
        if let Some(np) = new_parent {
            self.link_parent(id, np);
        }
        true
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — add a spatial relationship edge `from →<rel>→ to` (e.g.
    /// `ON`/`IN`/`NEAR`/`SUPPORTS`), idempotently. Returns `false` if either endpoint
    /// is absent or the same edge already exists. Runs under the held write guard.
    pub fn add_spatial_relation(&mut self, from: &str, to: &str, rel: &str) -> bool {
        if from == to
            || !self.topo.node_map.contains_key(from)
            || !self.topo.node_map.contains_key(to)
            || self.has_relationship_edge(from, to, rel)
        {
            return false;
        }
        match rmp_serde::to_vec_named(&serde_json::json!({"relationship": rel})) {
            Ok(e) => self.add_edge(from.to_string(), to.to_string(), e).is_ok(),
            Err(_) => false,
        }
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — attach (or replace) the axis-aligned bounding volume of scene
    /// object `id`. No-op returning `false` if the node is absent. Runs under the
    /// held write guard.
    pub fn set_bounding_volume(&mut self, id: &str, aabb: &crate::scene::Aabb) -> bool {
        let mut update = serde_json::Map::new();
        update.insert("aabb".to_string(), aabb.to_json());
        self.merge_fields(id, &update)
    }
}
