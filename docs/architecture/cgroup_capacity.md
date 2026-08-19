# Cgroup-aware capacity resolution

`eg-resource` is the single resource-capacity seam shared by the engine and
lower workspace providers. It combines the process's host-visible CPU affinity
and `MemTotal` observation with the cgroup controller hierarchy before any
automatic queue, concurrency, runtime, memory, or ASR limit is derived.

## Resolution contract

- The resolver reads `/proc/self/cgroup` and `/proc/self/mountinfo` to locate
  the process's actual v2 or v1 controller mount. It does not assume that
  `/sys/fs/cgroup` is the active mount or that the process is at its root.
- It walks every controller file from the mount root to the process cgroup.
  The smallest finite CPU quota ratio and memory limit remain effective even
  when a child reports `max`.
- A missing controller hierarchy, or a controller file that is absent at a
  hierarchy level where that controller is not enabled, is `Unavailable` and
  retains the host observation. A resolved controller file that is unreadable
  for any other reason, or whose content is malformed, is `Malformed` and uses
  the bounded fallback; it never silently restores an unbounded host-derived
  value. Probe metadata failures (`/proc/self/cgroup` or `mountinfo`) also fail
  closed.
- CPU affinity remains represented by `available_parallelism()`, while a finite
  cgroup quota is intersected with that observation. Automatic values reserve
  10% CPU and 20% RAM headroom.
- The resolved capacity is one immutable process-start snapshot. Runtime pools,
  queues, and connection admission all reuse it rather than re-reading `/proc`
  on hot paths. A deployment that changes cgroup limits in place restarts the
  engine so every process-lifetime pool is rebuilt from the same new bounds.
- Explicit values may lower an automatic cap but are clamped to it by
  `bound_explicit`; they cannot widen a constrained cgroup. This policy applies
  to runtime workers, admission/transport queues, writer/coalescer sizing,
  memory and mutation budgets, graph node caps, raft/redb derived counts, and
  the ASR thread budget.

The parser and hierarchy fixtures live beside the implementation in
`crates/eg-resource/src/lib.rs`. Consumer modules use the root compatibility
facade at `src/autosize.rs` or the leaf crate directly; they must not add a
second `/proc` or cgroup parser.
