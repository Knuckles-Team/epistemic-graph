use super::*;

impl GraphCore {
    // ── Action / policy / trajectory memory — one-shot wrappers + queries ─────
    //     (CONCEPT:EG-KG.compute.discounted-return)

    /// One-shot [`GraphTxn::start_trajectory`] (CONCEPT:EG-KG.compute.discounted-return): create/UPSERT the
    /// `:Trajectory` node under ONE topology write guard, then invalidate the lazy
    /// secondary indexes. Returns the trajectory id.
    pub fn start_trajectory(&self, props: serde_json::Map<String, serde_json::Value>) -> String {
        let id = self.txn().start_trajectory(props);
        self.mark_dirty();
        id
    }

    /// One-shot [`GraphTxn::append_step`] (CONCEPT:EG-KG.compute.discounted-return): create the `:Step`, link it
    /// (`BELONGS_TO` + `HAS_STEP` + `NEXT_STEP`), and advance the trajectory's
    /// tail/count — all under ONE topology write guard (atomic), then invalidate the
    /// lazy secondary indexes. Returns the new step id, or `None` if the trajectory is
    /// absent.
    #[allow(clippy::too_many_arguments)]
    pub fn append_step(
        &self,
        traj_id: &str,
        action: serde_json::Value,
        reward: f64,
        state_ref: Option<&str>,
        next_state_ref: Option<&str>,
        t: u64,
    ) -> Option<String> {
        let id = self
            .txn()
            .append_step(traj_id, action, reward, state_ref, next_state_ref, t);
        if id.is_some() {
            self.mark_dirty();
        }
        id
    }

    /// Decode a step node's stored property object (CONCEPT:EG-KG.compute.discounted-return read helper).
    /// `None` if the node is absent or its blob is not a decodable object.
    pub(super) fn step_object(
        &self,
        id: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let blob = self.get_node_properties(id)?;
        match decode_property_value(&blob) {
            Ok(serde_json::Value::Object(o)) => Some(o),
            _ => None,
        }
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the ordered `(step_id, step_object)` pairs of trajectory
    /// `traj_id`: the targets of its outgoing `HAS_STEP` edges, ordered by each step's
    /// `step_index` (append order). Ties on `step_index` fall back to the id for a
    /// deterministic total order. Empty if the trajectory is absent / has no steps.
    pub(super) fn ordered_steps(
        &self,
        traj_id: &str,
    ) -> Vec<(String, serde_json::Map<String, serde_json::Value>)> {
        let targets: Vec<String> = {
            let topo = self.topo.read();
            let Some(&idx) = topo.node_map.get(traj_id) else {
                return Vec::new();
            };
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        let mut steps: Vec<(String, serde_json::Map<String, serde_json::Value>)> = targets
            .into_iter()
            .filter(|t| self.edge_has_relationship(traj_id, t, "HAS_STEP"))
            .filter_map(|t| self.step_object(&t).map(|o| (t, o)))
            .collect();
        steps.sort_by(|a, b| {
            let ai = a.1.get("step_index").and_then(|v| v.as_u64()).unwrap_or(0);
            let bi = b.1.get("step_index").and_then(|v| v.as_u64()).unwrap_or(0);
            ai.cmp(&bi).then_with(|| a.0.cmp(&b.0))
        });
        steps
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the ordered step ids of trajectory `traj_id` (append order,
    /// following the `NEXT_STEP` chain). Empty if the trajectory is absent / has no
    /// steps.
    pub fn trajectory_steps(&self, traj_id: &str) -> Vec<String> {
        self.ordered_steps(traj_id)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the DISCOUNTED return of trajectory `traj_id`: `Σ gamma^t *
    /// reward` over its ordered steps, where `t` is each step's stored timestep and
    /// `reward` its reward (a step missing `reward` contributes 0; a step missing `t`
    /// reads as `t = 0`). With sequential timesteps `t = 0,1,2,…` this is the standard
    /// RL return `Σ gamma^i r_i`. Deterministic — `gamma` is caller-supplied, no
    /// clock/RNG. `0.0` for an absent / empty trajectory.
    pub fn discounted_return(&self, traj_id: &str, gamma: f64) -> f64 {
        self.ordered_steps(traj_id)
            .iter()
            .map(|(_, o)| {
                let reward = o.get("reward").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let t = o.get("t").and_then(|v| v.as_u64()).unwrap_or(0);
                gamma.powf(t as f64) * reward
            })
            .sum()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the UNDISCOUNTED total reward of trajectory `traj_id`: the plain
    /// sum of its steps' `reward` values (equivalent to `discounted_return` with
    /// `gamma = 1.0`). `0.0` for an absent / empty trajectory.
    pub fn total_reward(&self, traj_id: &str) -> f64 {
        self.ordered_steps(traj_id)
            .iter()
            .map(|(_, o)| o.get("reward").and_then(|v| v.as_f64()).unwrap_or(0.0))
            .sum()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — SLIDING-window undiscounted returns over trajectory `traj_id`:
    /// for ordered rewards `r_0 … r_{n-1}`, returns `[Σ r_i..i+window]` for each start
    /// `i` in `0..=n-window` (the n-step return at each position). Empty when `window`
    /// is 0 or larger than the step count. Deterministic. Useful for spotting the
    /// best/worst stretch of a long episode for prioritized replay.
    pub fn windowed_returns(&self, traj_id: &str, window: usize) -> Vec<f64> {
        let rewards: Vec<f64> = self
            .ordered_steps(traj_id)
            .iter()
            .map(|(_, o)| o.get("reward").and_then(|v| v.as_f64()).unwrap_or(0.0))
            .collect();
        if window == 0 || window > rewards.len() {
            return Vec::new();
        }
        (0..=rewards.len() - window)
            .map(|i| rewards[i..i + window].iter().sum())
            .collect()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the trajectory in `traj_ids` with the HIGHEST discounted return
    /// (for prioritized replay / policy selection). Ties are broken deterministically by
    /// the smaller id. `None` for an empty input. Absent trajectories score `0.0`.
    pub fn best_trajectory(&self, traj_ids: &[String], gamma: f64) -> Option<String> {
        traj_ids
            .iter()
            .map(|id| (id.clone(), self.discounted_return(id, gamma)))
            .min_by(|a, b| {
                // Max return, then min id — negate the return comparison for `min_by`.
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            })
            .map(|(id, _)| id)
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the trajectory in `traj_ids` with the LOWEST discounted return
    /// (for hard-negative mining / avoidance). Ties are broken deterministically by the
    /// smaller id. `None` for an empty input. Absent trajectories score `0.0`.
    pub fn worst_trajectory(&self, traj_ids: &[String], gamma: f64) -> Option<String> {
        traj_ids
            .iter()
            .map(|id| (id.clone(), self.discounted_return(id, gamma)))
            .min_by(|a, b| {
                a.1.partial_cmp(&b.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            })
            .map(|(id, _)| id)
    }
}
