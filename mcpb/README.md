# OptimusKG MCP Bundle

This is a [Desktop Extension](https://www.anthropic.com/engineering/desktop-extensions)
(`.mcpb`) file to query the [OptimusKG](https://optimuskg.ai/) biomedical knowledge graph in natural language.

## How it works

On first launch the bundle uses the published [optimuskg](https://pypi.org/project/optimuskg/) client to fetch the graph (~280 MB of Parquet) from Harvard Dataverse, then builds a local, indexed DuckDB database once. Every query after that runs
locally in milliseconds.

The graph data version is pinned per release via the `OPTIMUSKG_DOI` environment variable in `manifest.json`, so a given bundle always queries the same reproducible snapshot.

## What you can ask

- "What genes are associated with Alzheimer's disease, ranked by evidence?"
- "What does the drug aspirin target?"
- "How is aspirin connected to Alzheimer's disease?"
- "Which diseases have the most associated genes?"
- "What phenotypes are linked to the TP53 gene?"

## Tools

| Tool | What it does |
| --- | --- |
| `list_schema` | Node types, edge types, and the relation vocabulary of the graph. |
| `search_entities` | Resolve a name, gene symbol, synonym, or id to graph entities. |
| `get_entity` | Full properties of an entity plus a summary of its connections. |
| `get_neighbors` | One-hop neighbours, filterable by relation, edge type, direction, and `min_score`. |
| `count_neighbors` | Aggregate an entity's neighbours by edge type, relation, or neighbour type. |
| `find_connection` | Shortest path(s) between two entities (default and recommended **2 hops**). |
| `run_sql` | Read-only SQL escape hatch over the `nodes` / `edges` / `adj` tables for the long tail of questions. |

The graph is stored as three tables: `nodes(id, type, name, symbol, full_name,
search_blob, properties)`, an undirected, indexed `adj(s, t, edge_type,
relation, score, reverse)`, and an `edges` view over its forward half. Per-edge
JSON is not materialised (it would multiply the cache size); the parsed
association strength is kept as `score`.

## Install

1. Download `optimuskg.mcpb` from the [releases page](https://github.com/mims-harvard/optimuskg/releases).
2. Double-click it to install into Claude Desktop.
3. The first query triggers a one-time download + index build (see below); later
   queries are instant.

Requires [`uv`](https://docs.astral.sh/uv/) on your system.

## Measured performance

One-time index build in 45.0 s (190,531 nodes, 21,813,816 edges) for a 2.54 GB local DuckDB cache. After that every query below runs against the cached, indexed database.

Latencies are wall-clock, warm cache, measured in-process with [`pytest-benchmark`](https://pytest-benchmark.readthedocs.io/).

| Tool | Scenario | Median (ms) | Mean (ms) | Min (ms) | StdDev (ms) | Ops/s | Rounds |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `list_schema` | describe the graph schema | 36.01 | 36.13 | 35.01 | 0.64 | 28 | 29 |
| `search_entities` | gene symbol exact (TSPAN6) | 71.04 | 71.58 | 70.06 | 1.37 | 14 | 14 |
| `search_entities` | disease name (Alzheimer disease) | 28.16 | 27.38 | 22.84 | 2.66 | 37 | 43 |
| `search_entities` | substring (kinase) | 91.53 | 90.40 | 79.66 | 5.75 | 11 | 13 |
| `get_entity` | gene | 19.15 | 19.37 | 17.90 | 0.99 | 52 | 56 |
| `get_entity` | disease (high-degree) | 49.81 | 49.84 | 46.03 | 2.39 | 20 | 23 |
| `get_neighbors` | drug, all edges (limit 50) | 24.53 | 24.60 | 23.76 | 0.62 | 41 | 43 |
| `get_neighbors` | disease→gene, min_score | 29.60 | 29.84 | 27.65 | 1.45 | 34 | 37 |
| `count_neighbors` | hub gene, by edge type | 37.09 | 37.36 | 33.63 | 1.92 | 27 | 31 |
| `find_connection` | 2 hops, direct neighbour | 18.62 | 18.87 | 17.12 | 1.10 | 53 | 60 |
| `find_connection` | 2 hops, drug↔disease (hubs) | 62.41 | 61.76 | 57.89 | 2.21 | 16 | 18 |
| `find_connection` | 2 hops, gene↔disease | 55.10 | 55.69 | 52.61 | 2.30 | 18 | 20 |
| `run_sql` | group+join aggregation (top diseases) | 113.28 | 117.50 | 109.59 | 13.24 | 9 | 10 |

_Environment: Intel(R) Core(TM) Ultra 7 165U, Python 3.12.11. Regenerate with `make mcpb-profile`._

## Development

Everything lives under `mcpb/` and is isolated from the main OptimusKG pipeline.

```bash
# From the repo root:
make mcpb-test       # correctness tests against the real graph
make mcpb-profile    # run the benchmark suite and refresh the Performance section
make mcpb-build      # validate + pack -> mcpb/dist/optimuskg.mcpb

# Or directly, from mcpb/:
uv run --with '.[profile]' pytest tests/test_tools.py
uv run --with '.[profile]' optimuskg-mcp-profile
uv run --no-project --with typer python build.py
```

Tests and profiling fetch the graph through the `optimuskg` client (cached
locally after the first download), exactly as the bundle does at runtime. Set `OPTIMUSKG_MCP_NODES` and `OPTIMUSKG_MCP_EDGES` only if you want to override with local Parquet files.

## License

MIT.
