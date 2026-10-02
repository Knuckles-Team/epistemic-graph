# Recorded graph-shard layouts

Each `.txt` file here is what an earlier build of this crate persisted for a
fresh graph shard: the exact bytes of its owner manifest row and the names of
every table in the file. They are inputs to the lineage tests, which prove that
the predecessors declared in `src/owner/lineage.rs` are the layouts existing
files really have, and that the offline upgrades accept those files.

They are recorded outputs. Do not edit them, and do not regenerate one from a
newer commit: a fixture is only meaningful for the commit it was taken from.

| File | Taken from commit | Layout digest in the manifest | SHA-256 of the file |
|---|---|---|---|
| `graph-shard-before-enrichment.txt` | `e276b43ab7609d93d4e29c55c47598b432362a88` | `343ebe5be22fa54dfe5eb8284dd8bdee371ae57b346ade11a76e4cd83e0c842d` | `d331c98ea3749dd8c7e5ca178c990dfb2a51d3b67eac6cf6748308480aacb599` |
| `graph-shard-before-audit-requests.txt` | `5e4c61938b5f1746bfaf0f42c7fb79c4303680ae` | `01c219acd2bb2f1d523a1bd85eb08442cca03bfc975d60de2eff2b91e3425793` | `f1bee9f2fa78a6ef9c3aa5eb85e1682fc5bf5be4f728a2c0d75d06db5f8ee3cf` |

`5e4c61938b5f1746bfaf0f42c7fb79c4303680ae` was the head of the default branch
when the operation audit-append idempotency index (`audit_requests`) was added;
its layout is the one every graph shard written before that index has. The
same program run at `33c5c6459154be8b8356b93e66c54322a28715c4`, the commit that
introduced the four repository-enrichment tables, printed a byte-identical
file, so that layout was stable for the whole interval.

No fixture exists for the declared "before repository enrichment policy
revisions" predecessor (three enrichment tables): no commit in this
repository's history builds a graph shard with that table set, so there is no
build to record it from. Its table set and digest stay exactly as they were
declared.

## How a fixture is produced

```bash
git worktree add --detach ../fixture-source <commit>
cp crates/eg-storage/examples/dump_graph_shard_manifest.rs \
   ../fixture-source/crates/eg-storage/examples/
cd ../fixture-source
cargo run -q -p eg-storage --example dump_graph_shard_manifest > <fixture>.txt
```

The program creates a graph-shard owner file through the public
`StorageKernel::create_owner::<GraphShardOwner>` of the checked-out build, under
the fixed identity `physical:fixture:graph-shard-lineage`, then reads the file
back through plain `redb` and prints four lines:

* `identity=` the physical identity the file was created under;
* `manifest_hex=` the value of the `manifest` row of `mutation_owner_manifest`;
* `tables=` every table in the file, sorted, comma-separated;
* `multimap_tables=` every multimap table (none).

It uses no path, clock or random input, so the same commit always prints the
same bytes.
