---
name: optimuskg
description:
  Guide for using OptimusKG, the biomedical knowledge graph, through the
  `optimuskg` Python client. Use this when loading, querying, filtering, or
  analyzing OptimusKG data — genes, drugs, diseases, phenotypes, anatomy,
  pathways, and their relationships — as Polars DataFrames or a NetworkX graph,
  or when downloading the published graph from Harvard Dataverse.
---

# OptimusKG

OptimusKG is a modern multimodal biomedical knowledge graph (190,531 nodes
across 10 entity types, 21,813,816 edges across 27 relation types) integrating
65 resources grounded in 18 ontologies via the BioCypher framework and Biolink
Model. It is published on [Harvard Dataverse](https://doi.org/10.7910/DVN/IYNGEV)
as Apache Parquet files and consumed through the `optimuskg` PyPI client.

This skill covers **using the published graph** via the client. It is *not* for
developing the data pipeline in the `mims-harvard/optimuskg` repo — that work
uses the repo's own `/node-catalog-sync` skill.

## When to use

Use the `optimuskg` client when you need to:

- Download OptimusKG node/edge tables for analysis.
- Load the graph as **Polars DataFrames** or a **NetworkX `MultiDiGraph`**.
- Filter by entity type (Gene, Drug, Disease, …) or relation type.
- Build biomedical analyses, Graph-RAG, or ML over the graph.

Always prefer this client over hand-rolling Dataverse downloads — it resolves
file IDs automatically and caches locally.

## Installation

```bash
uv add optimuskg     # in a uv project (preferred — see the `uv` skill)
pip install optimuskg
```

The client depends on `polars` (DataFrames) and `networkx` (graph view).

## Quick start

```python
import optimuskg

# Download a specific file; returns its local cached path
local_path = optimuskg.get_file("nodes/gene.parquet")

# Read a single Parquet file as a Polars DataFrame
drugs = optimuskg.load_parquet("nodes/drug.parquet")

# Load nodes + edges as Polars DataFrames (lcc=True -> largest connected component only)
nodes, edges = optimuskg.load_graph(lcc=True)

# Load as a NetworkX MultiDiGraph with properties merged onto node/edge attrs
G = optimuskg.load_networkx(lcc=True)
```

## Choosing the right loader

| Need | Function | Returns |
|------|----------|---------|
| Just the file on disk | `get_file(path, *, force=False)` | `pathlib.Path` |
| One table as a DataFrame | `load_parquet(path, *, force=False, **read_parquet_kwargs)` | `pl.DataFrame` |
| Whole graph as DataFrames | `load_graph(*, lcc=False, force=False)` | `(nodes, edges)` tuple of `pl.DataFrame` |
| Whole graph as NetworkX | `load_networkx(*, lcc=False, force=False, parse_properties=True)` | `nx.MultiDiGraph` |

Notes:

- `load_parquet` forwards extra kwargs to `pl.read_parquet` — e.g. push down
  column selection: `optimuskg.load_parquet("nodes/drug.parquet", columns=["id"])`.
- `force=True` re-downloads even if the file is already cached.
- `load_networkx` always builds a `MultiDiGraph` regardless of edge
  directionality; call `G.to_undirected()` if you need an undirected view.
- **Memory:** loading the *full* graph into NetworkX needs several GB (190k
  nodes, 21M edges) and emits a warning. Pass `lcc=True` for a smaller,
  connected variant unless you specifically need every node.

## File paths

Paths mirror the catalog layout under `data/gold/kg/parquet/` in the source repo:

```python
optimuskg.get_file("nodes.parquet")                              # full nodes table
optimuskg.get_file("edges.parquet")                              # full edges table
optimuskg.get_file("largest_connected_component_nodes.parquet")  # LCC nodes
optimuskg.get_file("largest_connected_component_edges.parquet")  # LCC edges
optimuskg.get_file("nodes/gene.parquet")                         # only Gene (GEN) nodes
optimuskg.get_file("edges/disease_gene.parquet")                 # only DIS-GEN edges
```

`nodes/<type>.parquet` files use the lowercase entity name (`gene`, `drug`, …);
`edges/<a>_<b>.parquet` files use the lowercase node pair (`disease_gene`,
`drug_gene`, …).

## Graph schema (at a glance)

**Node table** columns: `id`, `label` (the type code, e.g. `GEN`), `properties`.
**Edge table** columns: `from`, `to`, `label` (e.g. `DIS-GEN`), `relation`
(e.g. `ASSOCIATED_WITH`), `undirected`, `properties`.

In the unified `nodes.parquet` / `edges.parquet` tables, `properties` is a JSON
string. In the stratified per-type files (`nodes/<type>.parquet`,
`edges/<label>.parquet`) it is expanded into native typed columns as a Polars
`Struct`.

The 10 node type codes:

| Label | Type | | Label | Type |
|-------|------|-|-------|------|
| `GEN` | Gene | | `ANA` | Anatomy |
| `DIS` | Disease | | `MFN` | Molecular Function |
| `BPO` | Biological Process | | `CCO` | Cellular Component |
| `PHE` | Phenotype | | `PWY` | Pathway |
| `DRG` | Drug | | `EXP` | Exposure |

For the full edge-label/relation taxonomy (all 27 edge types and their relation
strings) and per-type property fields, see
[`reference/graph-schema.md`](reference/graph-schema.md).

## Common patterns

Filter Polars DataFrames by type/relation:

```python
import polars as pl

nodes, edges = optimuskg.load_graph(lcc=True)
genes = nodes.filter(pl.col("label") == "GEN")
dis_gen = edges.filter(pl.col("relation") == "ASSOCIATED_WITH")
```

Filter a NetworkX graph (properties are merged onto attrs):

```python
G = optimuskg.load_networkx(lcc=True)

# Nodes by type code
genes = [n for n, a in G.nodes(data=True) if a["label"] == "GEN"]

# Edges by relation
expression = [
    (u, v) for u, v, a in G.edges(data=True)
    if a["relation"] == "EXPRESSION_PRESENT"
]
```

Pass `parse_properties=False` to `load_networkx` to keep `properties` as a raw
JSON string instead of merging parsed keys into the attribute dicts.

## Configuration

The client targets `doi:10.7910/DVN/IYNGEV` on `https://dataverse.harvard.edu`
by default, and caches downloads in `platformdirs.user_cache_dir("optimuskg")`
(`~/.cache/optimuskg` on Linux, `~/Library/Caches/optimuskg` on macOS). Cache
keys include the dataset version, so a new release invalidates it automatically.

Override from code or via environment variables:

```python
optimuskg.set_cache_dir("/data/optimuskg-cache")
optimuskg.set_doi("doi:10.7910/DVN/EXAMPLE")     # target a different release
optimuskg.set_server("https://dataverse.example.org")  # non-Harvard installation

# Read current settings
optimuskg.get_cache_dir(); optimuskg.get_doi(); optimuskg.get_server()
```

```bash
export OPTIMUSKG_CACHE_DIR=/data/optimuskg-cache
export OPTIMUSKG_DOI=doi:10.7910/DVN/EXAMPLE
export OPTIMUSKG_SERVER=https://dataverse.example.org
```

## Citing and license

- **Cite** OptimusKG when you use it in research — see
  https://optimuskg.ai/docs/citation and the dataset DOI `10.7910/DVN/IYNGEV`.
- **License:** the OptimusKG codebase is MIT. The integrated source datasets
  keep their own licenses and terms of use, which may restrict redistribution or
  commercial use of a given graph subset — review each source's terms. See
  https://optimuskg.ai/docs/license.

## Documentation

For full details (function signatures, per-type schemas, edge relations), read
the official docs — they expose machine-readable text files:

- https://optimuskg.ai/llms.txt — index of all doc pages
- https://optimuskg.ai/llms-full.txt — full documentation in one file
- https://optimuskg.ai/docs/optimuskg-client/reference — API reference
- https://github.com/mims-harvard/optimuskg — source repository
