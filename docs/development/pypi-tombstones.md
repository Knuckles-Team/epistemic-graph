# PyPI tombstones for retired distribution names

A tombstone is a real, publishable distribution that carries no code. It takes over a
name the project no longer ships under. The tombstone declares the current name as its
only dependency, so installing the retired name installs the current one. `pip` then
reports the switch instead of leaving the user on a frozen release.

Two names are tombstoned:

| Retired name | Tombstone version | Depends on | Lives in |
| --- | --- | --- | --- |
| `epistemic-graph` | 2.28.0 | `medulla>=2.27.9` | [`packaging/tombstones/epistemic-graph`](../../packaging/tombstones/epistemic-graph) |
| `epistemic-graph-server` | 2.28.0 | `medulla-stem>=2.27.9` | [`packaging/tombstones/epistemic-graph-server`](../../packaging/tombstones/epistemic-graph-server) |

```bash
pip install epistemic-graph         # resolves to medulla
pip install epistemic-graph-server  # resolves to medulla-stem
```

The Python import name moved with the distribution: `import epistemic_graph` no
longer resolves (hard cutover, no alias) — import `medulla` instead.
`from epistemic_graph_server import binary_path` still works, because the
`medulla-stem` package the tombstone installs provides it.

## When a tombstone is published

A maintainer runs it once per retired name, by hand, from `main` — never from a release
tag:

```bash
gh workflow run publish-tombstones.yml -f confirm=publish-tombstones
```

`.github/workflows/publish-tombstones.yml` builds each tree with `python -m build` and
uploads it with PyPI trusted publishing. It stays separate from `release.yml`.
`release.yml` publishes only from a canonical `v*` tag push: its uploader calls
`scripts/verify_release_tag.py` and refuses anything else. A tombstone is a one-off
manual act.

The workflow refuses to run unless:

* the repository is `Knuckles-Team/medulla`, and
* the `confirm` input is exactly `publish-tombstones`, and
* each built wheel contains nothing outside its `.dist-info/` directory.

Before the first run, a maintainer must point both PyPI projects' trusted publisher at
`Knuckles-Team/medulla` and `publish-tombstones.yml`, with an empty environment field.
Otherwise PyPI rejects the OIDC exchange as `invalid-publisher`.

The version is a fixed floor, not a value wired to `Cargo.toml`. It must sort above the
last release published under the retired name, which is 2.27.7. Nothing in the release
pipeline bumps it. Bump it only if the dependency floor has to rise.

## The retired project stays published

Do not delete the old PyPI project. Deletion frees the name for anyone, and the
tombstone is what keeps `pip install <retired name>` correct. Leave the project
published. The tombstone version wins the resolver because it is the newest release.
The older files stay available for anyone who pinned them.

Yank a file only when it is broken, and yank only that file. An exact `==` pin still
installs a yanked release, which is what a pinned consumer needs. The tombstone above it
is unaffected. Yanking the whole project to "hide" the old name does not work. PyPI keeps
serving the yanked files and the tombstone still resolves. That costs a working pin and
buys nothing.
