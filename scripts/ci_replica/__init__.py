"""The workflow-derived CI replica, split by responsibility.

`scripts/ci_gate_replica.py` stays the executable entry point and the module
every consumer loads; these modules own the cohesive parts it composes:

* :mod:`registry` — the hand-maintained classification surface (WorkflowSpec,
  WORKFLOW_REGISTRY, local overrides, statuses) and the build-affecting file
  set derived from it;
* :mod:`build_toolchain` — .cargo/config.toml external-binary discovery (GAP 2)
  and cargo-feature toolchain resolution (GAP 4);
* :mod:`workflow_plan` — parsing a workflow file into concrete per-leg steps;
* :mod:`drift` — the anti-drift consistency check over all of the above.

The dependency edges run one way: drift -> workflow_plan -> build_toolchain ->
registry.
"""
