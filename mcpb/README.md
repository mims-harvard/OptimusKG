# OptimusKG MCP Bundle

Query the [OptimusKG](https://optimuskg.ai/) biomedical knowledge graph —
**190K+ entities and 21.8M relationships** across genes, diseases, drugs,
pathways, phenotypes, anatomy, exposures, and Gene Ontology terms — in natural
language from Claude Desktop. **No database to host, no data to download or
import by hand.**

This is a [Desktop Extension](https://www.anthropic.com/engineering/desktop-extensions)
(`.mcpb`): download one file, double-click, done.

## How it works

```
Claude Desktop  ──stdio──▶  MCP server (this bundle)  ──▶  local DuckDB (indexed)
                                     │  first run only
                                     ▼
                       optimuskg PyPI client ──▶ Harvard Dataverse (gold Parquet)
```

On first launch the bundle uses the published [`optimuskg`](https://pypi.org/project/optimuskg/)
client to fetch the gold graph (~280 MB of Parquet) from Harvard Dataverse, then
builds a **local, indexed DuckDB database** once. Every query after that runs
locally in milliseconds. Claude translates your question into tool calls — there
is no text-to-Cypher step and nothing runs on a server.

The graph data version is pinned per release via the `OPTIMUSKG_DOI` value in
`manifest.json`, so a given bundle always queries the same reproducible snapshot.

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

Requires [`uv`](https://docs.astral.sh/uv/) on your system (the bundle runs
`uv run`, which manages Python 3.12 and the native dependencies — DuckDB,
pyarrow — with the correct platform wheels).

## Performance

<!-- METRICS:START -->

### Measured performance

One-time index build: **49.3 s** (192,443 nodes, 21,820,674 edges) → **2.51 GB** local DuckDB cache. After that every query below runs against the cached, indexed database.

Latencies are wall-clock, warm cache, measured in-process with [`pytest-benchmark`](https://pytest-benchmark.readthedocs.io/) (statistical sampling: repeated rounds, median reported).

| Tool | Scenario | Median (ms) | Mean (ms) | Min (ms) | StdDev (ms) | Ops/s | Rounds |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `list_schema` | describe the graph schema | 38.10 | 39.00 | 35.28 | 3.14 | 26 | 29 |
| `search_entities` | gene symbol exact (TSPAN6) | 88.17 | 84.51 | 73.93 | 6.10 | 12 | 14 |
| `search_entities` | disease name (Alzheimer disease) | 29.52 | 28.51 | 24.04 | 2.10 | 35 | 41 |
| `search_entities` | substring (kinase) | 95.19 | 96.22 | 83.94 | 12.13 | 10 | 12 |
| `get_entity` | gene | 19.93 | 20.30 | 18.46 | 1.72 | 49 | 54 |
| `get_entity` | disease (high-degree) | 90.11 | 90.00 | 85.52 | 2.00 | 11 | 12 |
| `get_neighbors` | drug, all edges (limit 50) | 27.21 | 27.52 | 23.77 | 2.39 | 36 | 42 |
| `get_neighbors` | disease→gene, min_score | 32.40 | 32.62 | 31.36 | 1.11 | 31 | 33 |
| `count_neighbors` | hub gene, by edge type | 38.76 | 39.15 | 34.31 | 2.59 | 26 | 29 |
| `find_connection` | 2 hops, direct neighbour | 20.07 | 20.23 | 18.88 | 0.95 | 49 | 54 |
| `find_connection` | 2 hops, drug↔disease (hubs) | 104.23 | 104.44 | 100.84 | 2.47 | 10 | 11 |
| `find_connection` | 2 hops, gene↔disease | 54.10 | 54.38 | 52.13 | 1.69 | 18 | 20 |
| `run_sql` | group+join aggregation (top diseases) | 116.46 | 119.19 | 113.75 | 5.64 | 8 | 10 |

_Environment: Intel(R) Core(TM) Ultra 7 165U, Python 3.12.11. Regenerate with `make mcpb-profile`._

<!-- METRICS:END -->

## Development

Everything lives under `mcpb/` and is isolated from the main OptimusKG pipeline
(the repo's own package is named `optimuskg`, which collides with the PyPI
client, so the bundle must be its own uv environment).

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

Tests and profiling run against the repository's own `data/gold/kg/parquet/`
output when present (no download), or set `OPTIMUSKG_MCP_NODES` /
`OPTIMUSKG_MCP_EDGES` to point at Parquet files elsewhere.

### Why profiling is not wired into the root `cli`

The bundle depends on the PyPI `optimuskg` **client**, whose import name
collides with this repository's own `optimuskg` **package**. They cannot coexist
in one environment, so the bundle — and therefore its profiling — runs in an
isolated uv env, exposed as the `optimuskg-mcp-profile` script and the
`make mcpb-profile` target rather than a subcommand of `uv run cli`.

## Profiling methodology

`make mcpb-profile` measures each tool end-to-end against the full 21.8M-edge
graph:

- **Framework:** [`pytest-benchmark`](https://pytest-benchmark.readthedocs.io/) —
  statistical sampling with warmup, automatic round calibration, and
  outlier-aware summaries. Each row reports the median across many rounds.
- **What is measured:** the actual `Graph` tool methods the MCP server calls, on
  a warm DuckDB cache — the same code path Claude exercises, so the numbers are
  representative rather than a reimplementation.
- **Reproducibility:** results, the one-time build cost, and the cache size are
  rendered straight from the benchmark JSON into this README; re-run the target
  to regenerate them on your hardware.

## License

MIT.
