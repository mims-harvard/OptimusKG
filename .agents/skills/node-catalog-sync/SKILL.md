---
name: node-catalog-sync
description: Enforce synchronization between Kedro node files and catalog YAML files in the OptimusKG project. Use when editing any Python node file under optimuskg/pipelines/*/nodes/, including modifying run() functions, changing node() inputs/outputs, adding/removing/renaming DataFrame columns, or changing column types. Also use when creating new nodes or deleting existing ones.
---

# Node–Catalog Sync

When a node file is edited, the corresponding catalog YAML files must be updated to stay in sync.

## Path Mapping

Node `outputs` are namespace-prefixed by `pipeline.py`. Use this table to locate catalog files:

| Layer | Node `outputs` | Catalog ID | YAML path | `filepath` |
|-------|---------------|------------|-----------|------------|
| Bronze | `"{src}.{name}"` | `bronze.{src}.{name}` | `conf/base/catalog/bronze/{src}/{name}.yml` | `data/bronze/{src}/{name}.parquet` |
| Silver nodes | `"nodes.{entity}"` | `silver.nodes.{entity}` | `conf/base/catalog/silver/nodes/{entity}.yml` | `data/silver/nodes/{entity}.parquet` |
| Silver edges | `"edges.{e1}_{e2}"` | `silver.edges.{e1}_{e2}` | `conf/base/catalog/silver/edges/{e1}_{e2}.yml` | `data/silver/edges/{e1}_{e2}.parquet` |
| Gold | `"kg.{fmt}"` | `gold.kg.{fmt}` | `conf/base/catalog/gold/{fmt}.yml` | `data/gold/kg/{fmt}/` |

Multiple outputs (list) produce one YAML file per output.

## Sync Workflow

Follow all 4 steps in order after every node file edit.

### Step 1: Identify affected catalog entries

1. Read the `node()` call in the edited file to find `outputs` (string or list).
2. Determine the namespace from the corresponding `pipeline.py` (e.g., `namespace="bronze"`).
3. Construct the full catalog ID: `{namespace}.{output}`.
4. Locate the YAML file using the path mapping table above.

### Step 2: Update dataset ID and filepath

Only if the `outputs` value in the `node()` call changed:

1. Update the YAML top-level key to the new full catalog ID.
2. Update `filepath` following the convention in the path mapping table.
3. Rename the YAML file to match the new output name.

### Step 3: Rerun node and sync catalog

1. Rerun the node: `uv run kedro run --nodes={node_name}`
2. Sync schema and checksum: `uv run cli sync-catalog --dataset {catalog_id}`
   - This reads the parquet file on disk and updates both `load_args.schema` and `metadata.checksum` in the YAML automatically.
   - Use `--dry-run` to preview changes first.
   - Use `--validate` to check without writing (useful in CI).
3. **Never delete the checksum property.**

### Step 4: Cascade downstream

1. List all downstream nodes using `DryRunner` (no execution, just shows the DAG):
   ```
   uv run kedro run --from-nodes={node_name} --runner=optimuskg.runners.DryRunner
   ```
2. Rerun the edited node and all its downstream dependents:
   ```
   uv run kedro run --from-nodes={node_name}
   ```
3. Sync all affected catalog entries:
   ```
   uv run cli sync-catalog
   ```

## Catalog YAML Structure

```yaml
{catalog_id}:
  type: optimuskg.datasets.polars.ParquetDataset
  filepath: data/{layer}/{path}.parquet
  load_args:
    schema:
      column_name: pl.Type
      struct_column:
        nested_field: pl.Type
  metadata:
    checksum: {blake2b_hex_digest}
    kedro-viz:
      layer: {layer}
```
