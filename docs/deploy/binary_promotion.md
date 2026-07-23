# Engine binary promotion

Use the deployment-neutral atomic promotion interface for a served
`epistemic-graph-server`.
Deployment paths, manager identities, service names, credentials, and trust
material come from runtime deployment configuration; do not commit them to the
repository or an operational report.

## Preconditions

- Build the standard `full` binary. Every served artifact must include `server`,
  `security`, and authoritative redb persistence.
- Compile one native artifact at a time on resource-constrained hosts.
- Provision `GRAPH_SERVICE_PERSIST_DIR`, the non-empty request secret, exact
  audience/tenant/policy revision, trusted signer registry, and TLS material before
  promotion.
- Verify that the target durable store already uses the current format. Promotion
  never imports an obsolete store or enables an alternate persistence backend.
- Provide an external `ENGINE_PROMOTION_HOOK` executable for the active
  orchestrator. The repository does not embed scheduler commands or topology.

## Promotion sequence

1. Build and test the release artifact:

   ```bash
   CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo build --locked --release
   cargo test --locked --release --workspace
   ```

2. Validate the artifact before installation. Record only its version, target,
   feature manifest, and cryptographic digest; never record a workstation path,
   account name, endpoint, or secret.

3. Promote through the repository-owned atomic installer and the
   deployment-owned hook:

   ```bash
   ENGINE_BIN_DEST=<deployment-owned-destination> \
   ENGINE_PROMOTION_HOOK=<deployment-owned-executable> \
     scripts/promote_engine.sh --activate --activate-consumers --verify
   ```

   `ENGINE_BIN_DEST`, `ENGINE_PROMOTION_HOOK`, and any hook configuration are
   runtime inputs. Store neither their resolved values nor hook output in source
   control. The script validates the candidate, takes an exclusive promotion
   lock, verifies the staged digest, retains one current-contract recovery
   artifact, and publishes with an atomic rename.

4. The hook receives `preflight`, `activate`, `activate-consumers`, `verify`, and
   `rollback` actions. It owns the scheduler-specific safe activation order and
   must prevent concurrent old/new writers from serving the same authority.

5. Wait for startup validation to complete. The service must reject readiness
   until authoritative redb, durable replay, signer keys, `eg2.` policy values,
   and any configured TLS/observability exporters are ready.

6. Restart dependent GraphOS/agent services through their normal orchestrator
   update so they reconnect with the same current `eg2.` authority contract.

7. Run live verification through the public interface. At minimum, prove:

   - TLS/native connectivity and an authenticated `eg2.` request;
   - one authorized read and write under normal durable RBAC;
   - an authorization denial for a missing scope or cross-tenant claim;
   - a replayed envelope is rejected after process restart;
   - the promoted capability executes, not merely that the socket is bound;
   - traces and metrics arrive at each configured observability destination.

## Fresh-store bootstrap

A new empty durable RBAC store permits only signer-backed `eg2.` self-registration
in `__commons__` as `System`, with no teams, roles, or delegation and exactly the
`security:bootstrap` scope. Perform it once with a trusted operation signer. After
the first rule commits, every identity/RBAC change requires normal admin policy.

## Failure handling

On an activation failure, the installer restores the immediately previous
current-contract artifact and calls the hook's `rollback` action. Preserve
diagnostics through the deployment's log collector and fix forward whenever the
wire, security, or durable-store contract changed. Never reintroduce removed
request envelopes, persistence modes, or data readers to make a rollback start.
