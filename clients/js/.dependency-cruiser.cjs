/**
 * Native dependency-cruiser policy for the thin JavaScript client.
 *
 * Keep this file beside the package it governs.  The CI/pre-push profile
 * invokes depcruise with this explicit config, so a developer's ambient
 * dependency-cruiser config cannot change the result.
 *
 * `node_modules` is deliberately not committed and the hook runs no `npm ci`,
 * so a *declared* external dependency is unresolvable here through no fault of
 * the source.  Failing on that reports a version-pinned manifest entry as an
 * architecture violation -- a tooling artifact, not debt.  The exemption below
 * is therefore DERIVED FROM package.json rather than hand-maintained: it is not
 * an allowlist that can drift, and it cannot hide anything the manifest does not
 * already declare.  Both cases that matter still fail:
 *
 *   * a broken LOCAL import (a typo'd relative path resolves to nothing); and
 *   * an UNDECLARED external package (imported but absent from package.json).
 *
 * Proven against a known-bad fixture for each case before adoption.
 */
const packageManifest = require("./package.json");

const declaredExternals = Object.keys({
  ...packageManifest.dependencies,
  ...packageManifest.peerDependencies,
  ...packageManifest.optionalDependencies,
});

// `@scope/name` -> `@scope/name` or `@scope/name/sub-path`, anchored, with every
// regex metacharacter in the package name escaped.
const declaredExternalPattern = declaredExternals.length
  ? `^(?:${declaredExternals
      .map((name) => name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join("|")})(?:/|$)`
  : null;

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
      comment:
        "Every import must resolve, except an external package this package's " +
        "own package.json declares (unresolvable only because node_modules is " +
        "not committed). A broken local path, or an undeclared package, fails.",
      severity: "error",
      from: {},
      to: {
        couldNotResolve: true,
        ...(declaredExternalPattern ? { pathNot: declaredExternalPattern } : {}),
      },
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
