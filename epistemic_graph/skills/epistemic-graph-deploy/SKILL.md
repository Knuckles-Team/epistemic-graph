---
name: epistemic-graph-deploy
skill_type: skill
description: >
  Promote and deploy the epistemic-graph engine (the AI-native database) in the live
  Kubernetes graph-os unified-image deployment, and restart its clients safely. Use when
  shipping a new engine build, rolling out Rust-side changes, or restarting graph-os and
  messaging to pick up a validated artifact. The engine is baked into graph-os-unified;
  engine changes therefore require a validated wheel, an immutable image digest, and a
  Kubernetes rollout. Wraps scripts/promote_engine.sh.
domain: operations
license: MIT
tags: [epistemic-graph, database, operations, deploy, promotion, kubernetes, unified-image]
metadata:
  author: Genius
  version: '0.1.0'
---

# epistemic-graph deploy / promote

The live production shape is a Kubernetes `graph-os` Deployment running the
`graph-os-unified` image. The image contains the `epistemic-graph-server` executable and
its Python package; the graph-os process starts that engine as its managed child. The
authoritative redb data lives on the deployment-owned durable volume. There is no supported
host-path binary hot-swap and no second standalone engine to scale up: an engine change is
an image change followed by a digest-pinned Kubernetes rollout.

The serving-plane source may be mounted from the canonical `agent-utilities` checkout.
That means an AU-only change can ship by restarting the Deployment, but it does not replace
the engine artifact in the image. Keep the engine image and the mounted AU revision as one
tested release pair. Full artifact details and rationale: `docs/deploy/binary_promotion.md`.

## Artifact helper and live delivery

`promote_engine.sh` is a deployment-neutral binary helper, not the live Kubernetes image
builder. Its `--build` option runs Cargo for a raw `epistemic-graph-server`; it does not
produce the Python wheel or the `graph-os-unified` image. This repository ships no live
Kubernetes promotion hook. Use the script for artifact staging/validation, or only with an
explicitly supplied deployment adapter:

```bash
ENGINE_BIN_DEST=<absolute-staging-destination> \
ENGINE_BUILD_FEATURES=full,ast-extended \
  scripts/promote_engine.sh --build
```

`ENGINE_BIN_DEST` is an absolute, non-symlink destination controlled by the caller.
`ENGINE_SOURCE_BINARY` may point at an already-built executable; otherwise `--build`
produces the current `ENGINE_BUILD_FEATURES` release (default `full,ast-extended`). The
current options are `--build`, `--activate`, `--activate-consumers`, and `--verify`.
The latter three require a regular executable supplied through `ENGINE_PROMOTION_HOOK`;
without that explicit adapter the script does not activate Kubernetes or restart clients.
If an adapter is supplied, it receives these bounded actions:

```text
preflight <candidate> <destination>
activate <destination>
activate-consumers <destination>
verify <destination>
rollback <destination>
```

The adapter, not `promote_engine.sh`, knows the cluster namespace, Deployment name, image
repository, registry credentials, or consumer set. The script does not run `kubectl`,
embed host names, or contain secrets. It validates the candidate with `--help`, invokes
`preflight` **before acquiring the exclusive promotion lock**, then atomically stages the
candidate, verifies the staged digest, retains the immediately previous artifact, and
invokes the remaining adapter actions in this order: `activate`, `activate-consumers`, then
`verify`.

## Kubernetes unified-image delivery

For an engine change, complete the following sequence in one bounded release lane. The
image build and the Kubernetes rollout are separate reviewed stages:

1. **Protect the authority.** Take the deployment's governed online redb backup and
   confirm that no other engine writer will share the durable store during activation.
   Do not change the persistence backend or format as part of an ordinary promotion.

2. **Build and gate one wheel.** On a dedicated build lane, build the standard
   `full,ast-extended` wheel with the repository's release tooling. Run the wheel privacy
   and completeness checks and record its version, source revision, and digest. Keep Cargo
   and image builds bounded: one heavy release build per builder and no shared target/cache
   writes from concurrent full builds. Do not substitute the raw server binary produced by
   `promote_engine.sh --build` for this wheel.

3. **Build the unified image.** Use the canonical image pipeline with the staged wheel and
   matching `agent-utilities` source revision. For local or development work, the
   parameterized Kubernetes/Kaniko job is
   `agent-packages/agent-utilities/docker/graphos-unified-kaniko-job.yaml`; for a rollout,
   prefer the internal GitLab `homelab/containers/images/graph-os-unified` pipeline. These
   run Kaniko in the cluster because the RKE2/containerd build hosts do not provide a
   Docker daemon. Publish the candidate only to the deployment's internal registry with a
   commit-derived, immutable tag; resolve and record the resulting digest. Never use a
   moving tag as the Deployment reference, and do not publish outside the internal
   validation path as part of this procedure.

4. **Review and apply the digest.** Update only the reviewed graph-os Deployment
   manifest or generated input owned by the deployment. Run the default client-side
   `kubectl diff` before applying it; this runbook does not claim server-side apply field
   ownership. Then apply the exact reviewed manifest and wait for the Recreate rollout:

   ```bash
   kubectl -n <namespace> diff -f <reviewed-graph-os-deployment.yaml>
   kubectl -n <namespace> apply -f <reviewed-graph-os-deployment.yaml>
   kubectl -n <namespace> rollout status deployment/<deployment> --timeout=900s
   ```

   Do not apply an incomplete aggregate manifest that would remove live containers,
   external-secret wiring, or durable volume definitions. The applied image digest and
   the engine/AU source pair must match the candidate that passed the gates.

5. **Refresh consumers after engine readiness.** After the rollout is ready, let an
   explicitly supplied adapter run `--activate-consumers`, or use the reviewed Kubernetes
   procedures for graph-os, messaging, and any other clients that must reconnect. Consumers
   must use the endpoint contract below; never start a local stand-in engine on a client
   node. Confirm consumer readiness before final verification.

6. **Verify the served path.** Check pod and consumer readiness with zero restart churn,
   then exercise an authenticated `eg2.` request through the public engine or graph-os
   path. Prove one authorized read and write, a missing-scope or cross-tenant denial, and
   the promoted capability itself. Health alone is insufficient: it must demonstrate that
   the new engine is serving. Check configured logs/metrics/traces for startup or protocol
   errors. This final verification is intentionally after consumer activation, matching the
   helper's `activate-consumers` then `verify` order.

## Endpoint and co-location contract

The live `graph-os` Deployment co-locates the image-baked engine child with the graph-os
process. Keep `GRAPH_SERVICE_ENDPOINTS=""` present but empty and retain the deployment's
`XDG_RUNTIME_DIR=/run/epistemic-graph` UDS setup so AgentConfig resolves the local socket
and autostarts the managed child. Do not replace that empty value with a remote endpoint or
delete it: the mounted configuration can otherwise re-inject a remote service and disable
the intended co-located path.

Remote/connect-only clients must use a **non-empty**, deployment-approved
`GRAPH_SERVICE_ENDPOINTS` value supplied by reviewed configuration. They must not inherit
the co-located empty value or start another engine. Restart clients only after the image
rollout and managed child are ready.

## Rollback

- **Preflight failure:** preflight runs before staging, so no artifact is changed and no
  rollback action is performed.
- **Activation failure:** the helper automatically restores the immediately previous
  staged artifact (or removes the destination if none existed) and invokes the adapter's
  `rollback` action. This is the helper's only automatic restoration path.
- **Consumer or final-verification failure:** the helper exits after the failing action and
  does not restore automatically. The operator must explicitly roll back the Kubernetes
  delivery: restore the previous image digest **and** matching `agent-utilities` source
  revision, review and apply the Deployment, wait for readiness, and reconnect consumers
  only after the managed child is healthy. If an adapter is in use, invoke its `rollback`
  action explicitly; do not assume the helper already did so.

An image-only rollback while `/au` still points at newer source can create an untested
pair. Preserve the durable redb data and never revive an obsolete persistence reader or an
additional writer as a rollback shortcut.

## Related
- `epistemic-graph-migrations` — snapshot/redb + config/schema migrations, version flips.
- `epistemic-graph-troubleshooting` — when a deploy leaves something unhealthy.
