/**
 * Native dependency-cruiser policy for the thin JavaScript client.
 *
 * Keep this file beside the package it governs.  The CI/pre-push profile
 * invokes depcruise with this explicit config, so a developer's ambient
 * dependency-cruiser config cannot change the result.
 */
module.exports = {
  forbidden: [
    {
      name: "no-circular",
      comment: "The thin client must remain a directed dependency graph.",
      severity: "error",
      from: {},
      to: { circular: true },
    },
    {
      name: "no-unresolved",
      comment: "Every client import must resolve in the checked-out package.",
      severity: "error",
      from: {},
      to: { couldNotResolve: true },
    },
  ],
  options: {
    doNotFollow: {
      path: "node_modules",
    },
    exclude: {
      path: "(^|/)(?:node_modules|dist|coverage|target)(/|$)",
    },
  },
};
