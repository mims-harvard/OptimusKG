# Known issues

## Pre-existing UBERON / ontology checksum drift

`uv run cli sync-catalog --layer silver --validate` reports four checksum
mismatches. None of them are caused by the relation-assertion work; all four
sit in the UBERON/ontology chain and their node logic is unchanged:

- `silver.edges.anatomy_anatomy`
- `silver.nodes.anatomy`
- `silver.nodes.disease`
- `silver.nodes.phenotype`

| catalog entry | recorded | locally regenerated |
| --- | --- | --- |
| `bronze.ontology.uberon_terms` | `30fd60ad…` | `9937f95f…` |
| `bronze.ontology.uberon_relations` | `28e39539…` | `baf3fb63…` |
| `silver.nodes.anatomy` | `33f03980…` | `1d25fe94…` |
| `silver.edges.anatomy_anatomy` | `8472f929…` | `136cac89…` |

What was established:

- The `anatomy_anatomy` builder is deterministic (reorder-stable) and its
  current code reproduces `136cac89` exactly.
- Commit `61cddba` bumped the `anatomy_anatomy` checksum from `136cac89` to
  `8472f929` **without changing the builder or its bronze inputs' code**.
- `conf/base/catalog/landing/ontology/uberon.yml` pins a release URL
  (`uberon v2026-04-01/human-view.json`) but no content hash, so the
  downloaded artifact can change under a fixed tag.

The most likely explanation is that the recorded checksums were produced from a
different UBERON artifact than the one currently in `data/landing`.

These checksums were deliberately **not** rewritten: doing so would assert that
the local data matches the published release, which has not been established.

Suggested resolution, in order of preference:

1. Pin a content hash for the UBERON landing artifact so the input is
   reproducible, then regenerate and record the resulting checksums.
2. Re-download the pinned release on a clean checkout and confirm which
   checksum it produces before updating the catalog.

## Verifying the relation-assertion guarantees

```bash
./scripts/rebuild_and_verify.sh          # full rebuild + every check
uv run python scripts/verify_relation_guarantees.py   # 21 correctness checks
uv run python scripts/check_determinism.py            # reproducibility
uv run python scripts/audit_relation_loss.py          # relation-loss audit
```

Run full pipelines in their own terminal window rather than as background jobs.
