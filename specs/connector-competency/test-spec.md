# Verification and acceptance contract

Tests run against disposable fixtures per connector; no live credential is part of the PR gate. Each `.3` row asserts that both its questions return a non-empty answer with provenance fields (source, record id, as-of), or a typed abstention naming the unconnected source when a linked connector has no data. Negative proof: a question whose required source is absent must abstain, never return an empty success.
