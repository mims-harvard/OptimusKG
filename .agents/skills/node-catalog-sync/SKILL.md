---
name: node-catalog-sync
description: >
  Enforces node-catalog synchronization in the OptimusKG Kedro project. Use this skill whenever
  editing, creating, or deleting a Python node file under `optimuskg/pipelines/{layer}/nodes/`.
  Ensures the corresponding catalog YAML files in `conf/base/catalog/{layer}/` are updated to
  match: schema, checksum, dataset ID, filepath, and downstream cascade. Also use when renaming
  node outputs, changing column definitions in a `run()` function, or adding/removing outputs
  from a node. Triggers on any modification to files matching `optimuskg/pipelines/*/nodes/*.py`.
---

# Node-Catalog Sync

When a node file is edited, the corresponding catalog YAML files **must** be updated and all downstream nodes rerun.

## Path Mapping

Node file: `optimuskg/pipelines/{layer}/nodes/{source}.py`
Catalog dir: `conf/base/catalog/{layer}/`

The `outputs` field in the `node()` call determines catalog file locations. The pipeline applies `namespace="{layer}"`, so output `"foo.bar"` becomes catalog ID `{layer}.foo.bar`.

| Layer | Node `outputs` | Catalog ID | YAML path | `filepath` |
|-------|---------------|-----------|-----------|------------|
| bronze | `"{source}.{name}"` | `bronze.{source}.{name}` | `conf/base/catalog/bronze/{source}/{name}.yml` | `data/bronze/{source}/{name}.parquet` |
| silver | `"nodes.{entity}"` | `silver.nodes.{entity}` | `conf/base/catalog/silver/nodes/{entity}.yml` | `data/silver/nodes/{entity}.parquet` |
| silver | `"edges.{e1}_{e2}"` | `silver.edges.{e1}_{e2}` | `conf/base/catalog/silver/edges/{e1}_{e2}.yml` | `data/silver/edges/{e1}_{e2}.parquet` |
| gold | `["kg.csv", "kg.parquet"]` | `gold.kg.csv`, `gold.kg.parquet` | `conf/base/catalog/gold/csv.yml`, `conf/base/catalog/gold/parquet.yml` | `data/gold/formats/{fmt}/` |

Multiple outputs (list) produce one YAML file per output.

## Workflow

### 1. Identify outputs

Read the `*_node = node(...)` call at the bottom of the edited file. Extract the `outputs` value (string or list). Determine the layer from the file path or `tags`.

### 2. Update catalog schema

If columns were added, removed, renamed, or types changed in the `run()` function, update `load_args.schema` in each corresponding YAML.

Schema uses Polars type strings. Nested structs use indented keys:

```yaml
load_args:
  schema:
    id: pl.String
    label: pl.String
    properties:
      name: pl.String
      xrefs: pl.List(pl.String)
```

### 3. Update dataset ID and filepath

If the `outputs` string changed, update the YAML top-level key and `filepath` to match. Rename the YAML file if needed.

### 4. Update checksum

After making code changes:

1. Rerun the node: `uv run kedro run --nodes={layer}.{node_name}`
2. Get new checksum: `uv run cli checksum {filepath}` (where `{filepath}` is the `filepath` value from the YAML, e.g. `data/silver/nodes/disease.parquet`)
3. Update `metadata.checksum` in the YAML with the new hex string

**Never delete the `metadata.checksum` field.**

### 5. Cascade downstream

Find all nodes that consume this node's outputs:

1. Grep for each output catalog ID across all node files and catalog files
2. Rerun each downstream node: `uv run kedro run --nodes={layer}.{node_name}`
3. Update checksums for each downstream output
4. Repeat recursively until no more downstream consumers exist

```bash
# Example: find consumers of "bronze.ontology.mondo_terms"
grep -r "bronze.ontology.mondo_terms" optimuskg/pipelines/ conf/base/catalog/
```

## Catalog YAML Template

```yaml
{layer}.{dotted.output.name}:
  type: optimuskg.datasets.polars.ParquetDataset
  filepath: data/{layer}/{slash/output/name}.parquet
  load_args:
    schema:
      # columns from run() return value
  metadata:
    checksum: {hex_from_cli}
    kedro-viz:
      layer: {layer}
```
