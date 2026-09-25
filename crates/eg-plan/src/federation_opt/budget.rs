//! The per-query network budget and its typed refusals (design §4.5).

use std::time::Instant;

/// Error-code prefix of a budget refusal: `FEDERATION_BUDGET_EXCEEDED:<dimension>: …`.
pub const BUDGET_EXCEEDED: &str = "FEDERATION_BUDGET_EXCEEDED";
/// Error code of a key-lookup-only source asked for rows without keys.
pub const REQUIRES_KEYS: &str = "FEDERATION_SOURCE_REQUIRES_KEYS";
/// Error code of a source whose single request hit its own row cap, so completeness cannot
/// be proven.
pub const RESULT_INCOMPLETE: &str = "FEDERATION_RESULT_INCOMPLETE";

/// Bounds one query may spend across every remote fragment. A request may narrow it
/// ([`Self::narrowed`]), never widen it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FederationBudget {
    /// Remote requests (round trips) across the query.
    pub max_requests: u32,
    /// Rows received from remote sources across the query.
    pub max_rows: usize,
    /// Local ids one bind join may ship as keys.
    pub max_bind_keys: usize,
    /// Wall-clock milliseconds spent in remote fragments.
    pub max_wall_ms: u64,
}

impl Default for FederationBudget {
    fn default() -> Self {
        Self {
            max_requests: 256,
            max_rows: 250_000,
            max_bind_keys: 10_000,
            max_wall_ms: 60_000,
        }
    }
}

impl FederationBudget {
    /// The server budget: the defaults, each overridable by
    /// `EPISTEMIC_GRAPH_FEDERATION_MAX_{REQUESTS,ROWS,BIND_KEYS,WALL_MS}`.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            max_requests: eg_types::runtime_limit::positive_from_env(
                "EPISTEMIC_GRAPH_FEDERATION_MAX_REQUESTS",
                d.max_requests,
            ),
            max_rows: eg_types::runtime_limit::positive_from_env(
                "EPISTEMIC_GRAPH_FEDERATION_MAX_ROWS",
                d.max_rows,
            ),
            max_bind_keys: eg_types::runtime_limit::positive_from_env(
                "EPISTEMIC_GRAPH_FEDERATION_MAX_BIND_KEYS",
                d.max_bind_keys,
            ),
            max_wall_ms: eg_types::runtime_limit::positive_from_env(
                "EPISTEMIC_GRAPH_FEDERATION_MAX_WALL_MS",
                d.max_wall_ms,
            ),
        }
    }

    /// The field-wise minimum of `self` and `other` — a caller can only tighten.
    pub fn narrowed(self, other: Self) -> Self {
        Self {
            max_requests: self.max_requests.min(other.max_requests),
            max_rows: self.max_rows.min(other.max_rows),
            max_bind_keys: self.max_bind_keys.min(other.max_bind_keys),
            max_wall_ms: self.max_wall_ms.min(other.max_wall_ms),
        }
    }
}

/// The typed refusal for one exhausted dimension.
pub(crate) fn refusal(dimension: &str, limit: impl std::fmt::Display) -> String {
    format!("{BUDGET_EXCEEDED}:{dimension}: the federated query exceeded its limit of {limit}")
}

/// What one query has spent so far.
#[derive(Debug)]
pub(crate) struct BudgetMeter {
    budget: FederationBudget,
    requests: u32,
    rows: usize,
    started: Instant,
}

impl BudgetMeter {
    pub(crate) fn new(budget: FederationBudget) -> Self {
        Self {
            budget,
            requests: 0,
            rows: 0,
            started: Instant::now(),
        }
    }

    pub(crate) fn budget(&self) -> FederationBudget {
        self.budget
    }

    /// Account one round trip before it is sent.
    pub(crate) fn charge_request(&mut self) -> Result<(), String> {
        if self.requests >= self.budget.max_requests {
            return Err(refusal("requests", self.budget.max_requests));
        }
        let elapsed = self.started.elapsed().as_millis();
        if elapsed > u128::from(self.budget.max_wall_ms) {
            return Err(refusal("wall_ms", self.budget.max_wall_ms));
        }
        self.requests += 1;
        Ok(())
    }

    /// Account rows received.
    pub(crate) fn charge_rows(&mut self, rows: usize) -> Result<(), String> {
        self.rows = self.rows.saturating_add(rows);
        if self.rows > self.budget.max_rows {
            return Err(refusal("rows", self.budget.max_rows));
        }
        Ok(())
    }
}
