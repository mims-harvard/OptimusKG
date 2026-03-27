# Evaluation Commands

This module provides CLI commands for generating evaluation datasets for knowledge graph analysis.

## Data Source

By default, commands use the **gold KG exports** at `data/gold/kg/parquet/`. Outputs are saved to `data/gold/evals/`.

## Available Commands

### `uv run cli evals centrality`

Compute node centrality scores for all nodes in the knowledge graph.

**Outputs:**
- `<metric>.csv` - Full rankings with node metadata
- `<metric>_by_type.pdf` - Bar chart of mean centrality by node type

**Usage:**
```bash
# Run with defaults (pagerank, undirected graph)
uv run cli evals centrality

# Compute degree centrality instead
uv run cli evals centrality --metric degree

# Run on directed graph only
uv run cli evals centrality --graph-mode directed

# Show top 20 nodes, custom output directory
uv run cli evals centrality --top 20 --out data/gold/evals/v2
```

**`--metric` options:** `pagerank` (default), `degree`, `betweenness`, `closeness`, `eigenvector`

**`--graph-mode` options:** `undirected` (default — all edges treated as bidirectional), `directed` (only edges with `undirected=true` in the parquet get a reverse arc)

---

### `uv run cli evals sample-edges`

Generate edge evaluation dataset for link prediction models.

**Methodology:**
1. **Centrality Computation**: Nodes are ranked by a chosen centrality metric within each type
2. **Node Sampling**: Nodes between top X% and top Y% are sampled (default: 10–20%)
3. **Edge Sampling**: For each seed node, true neighbors and false non-neighbors are sampled

**Outputs:**
- `<metric>_distribution_by_type.pdf` - Centrality vs rank plots per node type
- `sampled_nodes.csv` - Sampled seed nodes with metadata
- `sampled_edges.csv` - True and false edge pairs with labels
- `summary_stats.json` - Sampling statistics and all parameters

**Usage:**
```bash
# Run with config defaults
uv run cli evals sample-edges

# Use degree centrality on a directed graph, sample by degree
uv run cli evals sample-edges --metric degree --graph-mode directed --edge-sampling degree

# Override percentile range and node count
uv run cli evals sample-edges --centrality-upper 10 --centrality-lower 25 --nodes-per-type 200

# Use custom config file
uv run cli evals sample-edges --config conf/local/evals.yml

# Override seed
uv run cli evals sample-edges --seed 123
```

**`--metric` options:** `pagerank` (default), `degree`, `betweenness`, `closeness`, `eigenvector`

**`--graph-mode` options:** `undirected` (default), `directed`

**`--edge-sampling` options:**
- `uniform` (default) — each candidate neighbor/non-neighbor is equally likely to be selected
- `degree` — candidates are weighted by total degree (in + out), so higher-degree nodes are proportionally more likely to appear. Produces harder negatives for link-prediction evaluation.

## Configuration

Parameters for `sample-edges` are loaded from `conf/base/evals.yml`. Legacy `pagerank_upper` / `pagerank_lower` keys are still accepted.

```yaml
edge_eval:
  nodes_dir: data/gold/kg/parquet/nodes
  edges_dir: data/gold/kg/parquet/edges
  out_dir: data/gold/evals
  centrality_upper: 10     # Top 10% cutoff
  centrality_lower: 20     # Top 20% cutoff
  nodes_per_type: 100
  true_neighbors: 10
  false_neighbors: 5
  seed: 42
```

CLI arguments override config values.

## Output Schema

### `sampled_nodes.csv`

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | str | Node identifier (e.g., ENSG00000141510) |
| `node_type` | str | Node type (e.g., GEN, DIS, DRG) |
| `node_name` | str | Human-readable name (e.g., "TP53") |
| `centrality` | float | Centrality score (metric-dependent) |
| `rank_within_type` | int | Rank within node type (1 = highest) |
| `percentile` | float | Percentile within type (0 = top node) |

### `sampled_edges.csv`

| Column | Type | Description |
|--------|------|-------------|
| `seed_node_id` | str | The sampled seed node |
| `seed_node_type` | str | Type of seed node |
| `seed_node_name` | str | Name of seed node |
| `seed_centrality` | float | Centrality score of the seed node |
| `target_node_id` | str | The neighbor or non-neighbor |
| `target_node_type` | str | Type of target node |
| `target_node_name` | str | Name of target node |
| `is_true_edge` | bool | True if a real edge exists, False otherwise |
| `relation_type` | str | Edge relation label(s), pipe-separated if multiple |

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--nodes` | `data/gold/kg/parquet/nodes` | Directory containing node parquet files |
| `--edges` | `data/gold/kg/parquet/edges` | Directory containing edge parquet files |
| `--out` | `data/gold/evals` | Output directory |
| `--config` | `conf/base/evals.yml` | Path to config file |
| `--metric` | `pagerank` | Centrality metric for node ranking |
| `--graph-mode` | `undirected` | Edge directionality (`undirected` or `directed`) |
| `--edge-sampling` | `uniform` | Neighbor sampling strategy (`uniform` or `degree`) |
| `--centrality-upper` | config | Upper percentile cutoff (top X%) |
| `--centrality-lower` | config | Lower percentile cutoff (top X%) |
| `--nodes-per-type` | config | Nodes to sample per node type |
| `--true-neighbors` | config | Max true neighbors to sample per node |
| `--false-neighbors` | config | False neighbors to sample per node |
| `--seed` | config | Random seed for reproducibility |

## Edge Cases

- **Nodes with fewer neighbors than requested**: All available named neighbors are sampled
- **Empty percentile range**: An error is raised if no nodes fall within the range
- **Isolated nodes**: Nodes not in the graph are skipped during edge sampling
- **Degree sampling with all-zero weights**: Falls back to uniform sampling silently
- **Legacy config keys**: `pagerank_upper` / `pagerank_lower` in YAML are automatically mapped to `centrality_upper` / `centrality_lower`
