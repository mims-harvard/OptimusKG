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
| `find_connection` | Shortest path(s) between two entities. |
| `run_sql` | Read-only SQL query entrypoint over the DuckDB tables for the long tail of questions. |

## Install

1. Download `optimuskg.mcpb` from the [releases page](https://github.com/mims-harvard/optimuskg/releases).
2. Double-click it to install into Claude Desktop.
3. The first query triggers a one-time download + index build; later
   queries are instant.

Requires [`uv`](https://docs.astral.sh/uv/) on your system.

## Measured performance

One-time index build in 45.0 s (190,531 nodes, 21,813,816 edges) for a 2.54 GB local DuckDB cache. After that every query below runs against the cached, indexed database.

Latencies are wall-clock, warm cache, measured in-process with [`pytest-benchmark`](https://pytest-benchmark.readthedocs.io/).

| Tool | Scenario | Median (ms) | Mean (ms) | Min (ms) | StdDev (ms) | Ops/s | Rounds |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `list_schema` | describe the graph schema | 36.53 | 36.83 | 34.04 | 1.75 | 27 | 30 |
| `search_entities` | gene symbol exact (TSPAN6) | 82.98 | 82.36 | 76.33 | 4.49 | 12 | 14 |
| `search_entities` | disease name (Alzheimer disease) | 25.38 | 25.82 | 24.19 | 1.17 | 39 | 41 |
| `search_entities` | substring (kinase) | 85.49 | 90.55 | 81.83 | 9.84 | 11 | 13 |
| `get_entity` | gene | 19.37 | 19.66 | 17.84 | 1.24 | 51 | 56 |
| `get_entity` | disease (high-degree) | 49.20 | 50.89 | 44.14 | 5.47 | 20 | 22 |
| `get_neighbors` | drug, all edges (limit 50) | 28.40 | 28.28 | 24.27 | 1.63 | 35 | 40 |
| `get_neighbors` | disease→gene, min_score | 29.68 | 29.78 | 27.71 | 1.09 | 34 | 36 |
| `count_neighbors` | hub gene, by edge type | 36.77 | 37.82 | 32.38 | 3.25 | 26 | 30 |
| `find_connection` | 2 hops, direct neighbour | 19.22 | 19.58 | 17.95 | 1.52 | 51 | 57 |
| `find_connection` | 2 hops, drug↔disease (hubs) | 60.69 | 62.48 | 56.54 | 5.50 | 16 | 18 |
| `find_connection` | 2 hops, gene↔disease | 55.89 | 55.74 | 52.09 | 2.04 | 18 | 19 |
| `run_sql` | group+join aggregation (top diseases) | 121.90 | 120.90 | 110.62 | 6.59 | 8 | 9 |

_Environment: Intel(R) Core(TM) Ultra 7 165U, Python 3.12.11. Regenerate with `make mcpb-profile`._

## Development

Everything lives under `mcpb/` and is isolated from the main OptimusKG pipeline.

```bash
# From the repo root:
make mcpb-test       # correctness tests against the real graph
make mcpb-profile    # re-run the benchmarks and refresh the table above
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
