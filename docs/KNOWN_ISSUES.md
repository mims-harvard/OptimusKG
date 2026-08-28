# Known issues

## Resolved: UBERON / ontology checksum drift

Previously the `anatomy_anatomy`, `anatomy_gene`, `anatomy`, `disease` and
`phenotype` outputs had catalog checksums that did not match locally
regenerated data. The root cause was a landing catalog that pinned a release
URL without a content hash, so the downloaded artifact could change under a
fixed tag.

This was fixed on `main` by #213 (pin floating landing origins and correct the
UBERON checksum). After rebuilding, `cli sync-catalog --validate` reports
**0 mismatches** across bronze and silver.

If it reappears, re-check that `conf/base/catalog/landing/ontology/uberon.yml`
still pins an immutable artifact.

## Verifying the relation-assertion guarantees

The one-edge-per-node-pair invariant is lossless: every edge keeps the full set
of source-specific assertions in `properties.relation_assertions`, and flags
mutually exclusive ones via `properties.relation_conflict`.

```bash
uv run python scripts/verify_relation_guarantees.py   # 27 correctness checks
uv run python scripts/check_determinism.py            # reproducibility
uv run python scripts/audit_relation_loss.py          # relation-loss audit
uv run cli sync-catalog --validate                    # schemas + checksums
```

These checks are negative-controlled: injecting the old lossy collapse into
`drug_disease` makes R1/R2/R5/R6 fail (2,135 assertions missing, 573 edges
misresolved), and reverting a single `.sort()` makes `drug_gene` and
`exposure_gene` reorder-unstable again. `tests/test_relation_resolution.py`
pins the same properties as fast unit-level regression tests.

### Determinism

`group_by().agg()` with a bare `.unique()` returns list elements in a
non-deterministic order, so two identical runs produce different parquet bytes
and checksum verification fails. Every such aggregation must be `.sort()`ed
(or `.list.sort()`ed for list columns). `scripts/check_determinism.py` guards
this by re-running each builder with reversed inputs.

Run full pipelines in their own terminal window rather than as background jobs.
