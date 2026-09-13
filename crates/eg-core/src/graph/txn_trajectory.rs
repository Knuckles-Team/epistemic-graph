use super::*;

impl<'a> GraphTxn<'a> {
    // ── Action / policy / trajectory memory (CONCEPT:EG-KG.compute.discounted-return) ──────────────────
    //
    // Episodic TRAJECTORY memory for agents / robotics (the Phase-P counterpart to
    // the EG-220/221/222 memory tier and the EG-087 scene model). A `:Trajectory`
    // node heads an ordered chain of `:Step` nodes, each carrying `action`, `reward`,
    // `t` (the caller's timestep), and optional `state_ref` / `next_state_ref` that
    // reference EG-087 scene-object / state nodes. Steps are stitched into a temporal
    // chain with `NEXT_STEP` edges (tail → new), tied back to their trajectory with a
    // `BELONGS_TO` edge (step → trajectory), and made cheaply enumerable with a
    // reciprocal `HAS_STEP` edge (trajectory → step) — exactly the reciprocal-edge
    // convention EG-087 uses for `CHILD_OF`/`HAS_CHILD`. The trajectory node tracks
    // `step_count`, `head_step`, and `tail_step` so an append is O(1).
    //
    // Deterministic + atomic like the rest of the tier: the caller supplies every
    // number (`reward`, `t`, and the analysis `gamma`) — the engine reads NO clock and
    // NO RNG, so a run replays identically from the WAL / on a Raft follower. Ids are
    // derived from graph state + inputs. Everything below runs under the single held
    // topology write guard, so a `start_trajectory` / `append_step` is atomic w.r.t.
    // other writers. Backward-compatible: a `:Step`/`:Trajectory` is an ordinary node,
    // and the analysis helpers read only nodes reachable from the given trajectory.

    /// Deterministic id for a new trajectory over `(sequence, props)`
    /// (CONCEPT:EG-KG.compute.discounted-return). `sequence` is the live node count at insertion time, which is
    /// monotonic under replay, so identical WAL replay yields the identical id while
    /// distinct inserts never collide. No RNG/clock.
    pub(super) fn derive_trajectory_id(
        sequence: usize,
        props: &serde_json::Map<String, serde_json::Value>,
    ) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sequence.hash(&mut h);
        serde_json::Value::Object(props.clone())
            .to_string()
            .hash(&mut h);
        format!("trajectory:{:016x}", h.finish())
    }

    /// Deterministic id for the `step_index`-th step of trajectory `traj_id`
    /// (CONCEPT:EG-KG.compute.discounted-return): `"<traj_id>:step:<index>"`. Fully replayable (a function of
    /// the trajectory id + the monotonic step ordinal), unique within a trajectory,
    /// and it sorts lexicographically in append order. No RNG/clock.
    pub(super) fn derive_step_id(traj_id: &str, step_index: u64) -> String {
        format!("{}:step:{:08}", traj_id, step_index)
    }

    /// CONCEPT:EG-KG.compute.discounted-return — START a new `:Trajectory` (episode) and return its id. The
    /// caller-supplied `props` are stored verbatim; the engine injects the structural
    /// markers `type = "Trajectory"` and initializes `step_count = 0` (only when
    /// absent, so a re-run UPSERTS without resetting an in-progress episode). If
    /// `props` carries an `id` string it is honoured (and stripped from the stored
    /// blob); otherwise a deterministic id over `(live node count, props)` is used.
    /// Runs under the held write guard.
    pub fn start_trajectory(
        &mut self,
        props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let id = match props.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => Self::derive_trajectory_id(self.topo.node_map.len(), &props),
        };
        let mut obj = self.node_object(&id).unwrap_or_default();
        for (k, v) in props {
            if k == "id" {
                continue; // the node id, not a stored property
            }
            obj.insert(k, v);
        }
        obj.entry("type".to_string())
            .or_insert_with(|| serde_json::json!("Trajectory"));
        obj.entry("step_count".to_string())
            .or_insert_with(|| serde_json::json!(0));
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            self.add_node(id.clone(), blob);
        }
        id
    }

    /// CONCEPT:EG-KG.compute.discounted-return — APPEND a `:Step{ action, reward, t, state_ref, next_state_ref }`
    /// to trajectory `traj_id`, extending its ordered temporal chain. The new step is:
    /// * created with the caller's `action` (stored verbatim — a string or a
    ///   structured JSON action), `reward`, `t` (the caller's timestep), its ordinal
    ///   `step_index`, and any `state_ref` / `next_state_ref` (references to EG-087
    ///   scene / state nodes; omitted when `None`);
    /// * linked to its trajectory with a `BELONGS_TO` edge (step → trajectory) plus a
    ///   reciprocal `HAS_STEP` edge (trajectory → step) for O(matches) enumeration;
    /// * stitched onto the chain tail with a `NEXT_STEP` edge (previous tail → new
    ///   step); the first step also stamps the trajectory's `head_step`.
    ///
    /// The trajectory's `tail_step` and `step_count` are advanced. Returns the new
    /// step id, or `None` if `traj_id` is absent/undecodable (no partial write).
    /// Deterministic (no clock/RNG — `reward`/`t` are caller-supplied) and atomic under
    /// the held write guard. It is an accumulator (each call is a distinct step), so it
    /// is intentionally NOT idempotent.
    #[allow(clippy::too_many_arguments)]
    pub fn append_step(
        &mut self,
        traj_id: &str,
        action: serde_json::Value,
        reward: f64,
        state_ref: Option<&str>,
        next_state_ref: Option<&str>,
        t: u64,
    ) -> Option<String> {
        let traj = self.node_object(traj_id)?;
        let step_index = traj.get("step_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let tail = traj
            .get("tail_step")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let step_id = Self::derive_step_id(traj_id, step_index);

        self.write_step_node(
            &step_id,
            action,
            reward,
            state_ref,
            next_state_ref,
            t,
            step_index,
        );
        self.link_step(&step_id, traj_id, tail.as_deref());
        self.advance_trajectory(traj_id, &step_id, step_index, tail.as_deref());
        Some(step_id)
    }

    fn write_step_node(
        &mut self,
        step_id: &str,
        action: serde_json::Value,
        reward: f64,
        state_ref: Option<&str>,
        next_state_ref: Option<&str>,
        t: u64,
        step_index: u64,
    ) {
        let mut step = serde_json::Map::new();
        step.insert("type".to_string(), serde_json::json!("Step"));
        step.insert("action".to_string(), action);
        step.insert("reward".to_string(), serde_json::json!(reward));
        step.insert("t".to_string(), serde_json::json!(t));
        step.insert("step_index".to_string(), serde_json::json!(step_index));
        if let Some(s) = state_ref {
            step.insert("state_ref".to_string(), serde_json::json!(s));
        }
        if let Some(s) = next_state_ref {
            step.insert("next_state_ref".to_string(), serde_json::json!(s));
        }
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(step)) {
            self.add_node(step_id.to_string(), blob);
        }
    }

    fn link_step(&mut self, step_id: &str, traj_id: &str, tail: Option<&str>) {
        // BELONGS_TO (step → trajectory) + reciprocal HAS_STEP (trajectory → step).
        if let Ok(e) = rmp_serde::to_vec_named(&serde_json::json!({"relationship": "BELONGS_TO"})) {
            let _ = self.add_edge(step_id.to_string(), traj_id.to_string(), e);
        }
        if let Ok(e) = rmp_serde::to_vec_named(&serde_json::json!({"relationship": "HAS_STEP"})) {
            let _ = self.add_edge(traj_id.to_string(), step_id.to_string(), e);
        }

        // NEXT_STEP (previous tail → new step) — the temporal chain link.
        if let Some(prev) = tail {
            if let Ok(e) =
                rmp_serde::to_vec_named(&serde_json::json!({"relationship": "NEXT_STEP"}))
            {
                let _ = self.add_edge(prev.to_string(), step_id.to_string(), e);
            }
        }
    }

    fn advance_trajectory(
        &mut self,
        traj_id: &str,
        step_id: &str,
        step_index: u64,
        tail: Option<&str>,
    ) {
        // Advance the trajectory bookkeeping (head on the first step, always tail).
        let mut upd = serde_json::Map::new();
        upd.insert("tail_step".to_string(), serde_json::json!(step_id));
        upd.insert("step_count".to_string(), serde_json::json!(step_index + 1));
        if tail.is_none() {
            upd.insert("head_step".to_string(), serde_json::json!(step_id));
        }
        self.merge_fields(traj_id, &upd);
    }
}
