# EH-497: spectral clustering through MineCluster

This contract adds `algorithm="spectral"` to the existing `MineCluster` method. It does not restore the retired `SpectralCluster` method. The intended caller is Agent Utilities' `SpectralClusterNavigator`, which currently computes a spectral partition in Python. That caller remains on its existing path until the native method has generated contract, focused gates, and an explicit adapter with parity tests.

## Request and admission

Spectral requires explicit `features`; graph-derived `source` and `plan` are rejected when used as its input. This keeps the bounded kernel from triggering an unbounded graph scan. `k` bounds eigengap selection; for two or more rows the kernel selects at least two clusters even if `k < 2`, matching the AU navigator. A singleton yields one cluster and an empty matrix yields none. `seed` controls the final k-means step. `eps`, `min_pts`, `linkage`, and `max_iter` have no spectral effect. The handler validates a rectangular finite matrix, rejects more than 64 rows, and rejects `writeback=true`. Spectral calls are read-only, so no spectral record is admitted to the writeback WAL or replayed. Other `MineCluster` algorithms retain their graph-derived inputs and writeback behavior.

## Numerical result

The kernel constructs a nonnegative cosine affinity, builds the normalized graph Laplacian, diagonalizes it with a bounded Jacobi solver, selects the eigengap, and groups rows with seeded k-means. It returns the existing `labels`, row references, centroids, and compactness `score`. Each spectral cluster also carries `coherence`, its mean pairwise cosine similarity. This score can be negative if a cluster contains opposing vectors; the nonnegative clamp applies only to affinity. Non-spectral results omit that optional field. For explicit features, `members` are row indices; graph-derived rows use node IDs as before. Cluster IDs and member ordering are deterministic for the same input and seed.

The 64-row limit bounds the quadratic affinity matrix and Jacobi work. This is a deliberately small native contract for the current AU call site; larger graph partitions require a separate scalable design and admission policy.

## AU migration gate

The AU adapter must map native row indices back to its requested domain IDs, preserve its minimum cluster size and output shape, and compare partition and coherence behavior on fixed fixtures. Its existing `CommunityNode` persistence remains separate: native `MineCommunity` has different identity, edge direction, minimum size, and density semantics. Neither AU community persistence nor unrelated reasoning code may be deleted on the strength of this spectral contract alone.
