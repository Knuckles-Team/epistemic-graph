# Planted stale claim fixture

| ID | Status | Source anchor | Required source text | Evidence / scope |
|---|---|---|---|---|
| `m1.memory-budget-zero` | `IMPLEMENTED` | `src/cost/mod.rs#MEDULLA_MEMORY_BUDGET` | `explicit 0 disables it` | Deliberately stale copy of the historical claim; the checker must reject it. |
