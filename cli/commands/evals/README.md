# Evaluation Commands

This module provides CLI commands for generating evaluation datasets for knowledge graph analysis.

## Data Source

By default, commands use the **gold KG exports** at `data/gold/kg/parquet/`. Outputs are saved to `data/gold/evals/`.

## Available Commands

### `uv run cli evals pagerank`

Compute PageRank importance scores for all nodes in the knowledge graph.

**Outputs:**
- `pagerank.csv` - Full rankings with node metadata
- `pagerank_by_type.pdf` - Bar chart of mean PageRank by node type

**Usage:**
```bash
# Run with defaults
uv run cli evals pagerank

# Show top 20 nodes
uv run cli evals pagerank --top 20

# Custom output directory
uv run cli evals pagerank --out data/gold/evals/v2
```

### `uv run cli evals sample-edges`

Generate edge evaluation dataset for link prediction models.

**Methodology:**
1. **PageRank Computation**: Nodes are ranked by PageRank within each type
2. **Node Sampling**: Nodes between top X% and top Y% are sampled (default: 5-15%)
3. **Edge Sampling**: For each seed node, true neighbors and false non-neighbors are sampled

**Outputs:**
- `pagerank_distribution_by_type.pdf` - PageRank vs rank plots per node type
- `sampled_nodes.csv` - Sampled seed nodes with metadata
- `sampled_edges.csv` - True and false edge pairs with labels
- `summary_stats.json` - Sampling statistics

**Usage:**
```bash
# Run with config defaults
uv run cli evals sample-edges

# Override percentile range
uv run cli evals sample-edges --pagerank-upper 10 --pagerank-lower 25

# Use custom config file
uv run cli evals sample-edges --config conf/local/evals.yml

# Override specific parameters
uv run cli evals sample-edges --nodes-per-type 200 --seed 123
```

## Configuration

Parameters for `sample-edges` are loaded from `conf/base/evals.yml`:

```yaml
edge_eval:
  nodes_dir: data/gold/kg/parquet/nodes
  edges_dir: data/gold/kg/parquet/edges
  out_dir: data/gold/evals
  pagerank_upper: 5      # Top 5% cutoff
  pagerank_lower: 15     # Top 15% cutoff
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
| `node_type` | str | Node type (gene, disease, drug, etc.) |
| `node_name` | str | Human-readable name (e.g., "TP53") |
| `pagerank` | float | PageRank centrality score |
| `rank_within_type` | int | Rank within node type (1 = highest) |
| `percentile` | float | Percentile within type (0 = top node) |

### `sampled_edges.csv`

| Column | Type | Description |
|--------|------|-------------|
| `seed_node_id` | str | The sampled seed node |
| `seed_node_type` | str | Type of seed node |
| `seed_node_name` | str | Name of seed node |
| `target_node_id` | str | The neighbor or non-neighbor |
| `target_node_type` | str | Type of target node |
| `target_node_name` | str | Name of target node |
| `is_true_edge` | bool | True if real edge exists, False otherwise |
| `edge_type` | str | "true_neighbor" or "false_neighbor" |

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--nodes` | `data/gold/kg/parquet/nodes` | Directory containing node parquet files |
| `--edges` | `data/gold/kg/parquet/edges` | Directory containing edge parquet files |
| `--out` | `data/gold/evals` | Output directory |
| `--config` | `conf/base/evals.yml` | Path to config file |
| `--pagerank-upper` | config | Upper percentile cutoff (top X%) |
| `--pagerank-lower` | config | Lower percentile cutoff (top X%) |
| `--nodes-per-type` | config | Nodes to sample per node type |
| `--true-neighbors` | config | Max true neighbors to sample per node |
| `--false-neighbors` | config | False neighbors to sample per node |
| `--seed` | config | Random seed for reproducibility |

## Edge Cases

- **Nodes with fewer than 10 neighbors**: All available neighbors are sampled
- **Empty percentile range**: An error is raised if no nodes fall within the range
- **Isolated nodes**: Nodes not in the graph are skipped during edge sampling
