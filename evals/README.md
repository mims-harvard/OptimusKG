# Edge Evaluation Dataset

This module generates evaluation datasets for testing OptimusKG edge validity.

## Purpose

The edge evaluation dataset is created by sampling nodes from the knowledge graph based on their PageRank centrality and creating labeled edge pairs (true edges and false/negative edges) for evaluation. By default, evals use the `gold` KG exports at `data/gold/kg/parquet/`.

## Methodology

### Step 1: PageRank Computation

All nodes in the graph are ranked by PageRank score within their respective node types (gene, disease, drug, etc.). PageRank captures the importance of nodes based on the graph structure.

### Step 2: Node Sampling

Nodes are sampled from a specific PageRank percentile range to focus on moderately important nodes (avoiding both highly central hub nodes and peripheral low-degree nodes). The default range is between top 5% and top 15% of nodes per type.

- **Default range**: Between top 5% and top 15% of nodes per type
- **Sampling**: 100 nodes randomly sampled from eligible nodes per type
- **Total**: ~1000 nodes (100 per type × 10 node types)

The percentile-based sampling ensures:
- Nodes have enough neighbors for meaningful evaluation
- Nodes are not extreme outliers (hubs or isolates)
- Balanced representation across node types

### Step 3: Edge Sampling

For each sampled "seed" node:

1. **True Neighbors**: Up to 10 actual neighbors are randomly sampled from the node's adjacency list
2. **False Neighbors**: 5 non-neighbors are randomly sampled from all nodes in the graph

## Output Files

All outputs are saved to `evals/outputs/` (or custom `--out` directory):

### `pagerank_distribution_by_type.pdf`

Multi-panel figure showing PageRank score vs. rank for each node type.

### `sampled_nodes.csv`

The 1000 sampled seed nodes with metadata:

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | str | Node identifier (e.g., ENSG00000141510) |
| `node_type` | str | Node type (gene, disease, drug, etc.) |
| `node_name` | str | Human-readable name (e.g., "TP53") |
| `pagerank` | float | PageRank centrality score |
| `rank_within_type` | int | Rank within node type (1 = highest) |
| `percentile` | float | Percentile within type (0 = top node) |

### `sampled_edges.csv`

Edge evaluation pairs with full metadata:

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

### `summary_stats.json`

Comprehensive statistics about the sampling process:

```json
{
  "parameters": {
    "pagerank_upper_percentile": 5,
    "pagerank_lower_percentile": 15,
    "nodes_per_type": 100,
    "true_neighbors_per_node": 10,
    "false_neighbors_per_node": 5,
    "random_seed": 42
  },
  "graph": {
    "total_nodes": 123456,
    "total_edges": 789012
  },
  "node_sampling": {
    "total_sampled_nodes": 1000,
    "by_type": { ... }
  },
  "edge_sampling": {
    "total_true_edges": 9876,
    "total_false_edges": 5000,
    "nodes_with_fewer_than_max_neighbors": 45,
    "average_neighbors_per_seed": 87.3,
    "by_seed_type": { ... }
  }
}
```

## Usage

### CLI Command

```bash
# Run with default parameters
uv run cli edge-eval

# Custom percentile range (top 10% to top 25%)
uv run cli edge-eval --pagerank-upper 10 --pagerank-lower 25

# Sample more nodes per type
uv run cli edge-eval --nodes-per-type 200

# Custom output directory
uv run cli edge-eval --out evals/outputs/experiment_v2

# Full custom configuration
uv run cli edge-eval \
    --pagerank-upper 5 \
    --pagerank-lower 15 \
    --nodes-per-type 100 \
    --true-neighbors 10 \
    --false-neighbors 5 \
    --seed 42
```

### Python API

```python
from pathlib import Path
from evals.edge_eval import run

run(
    nodes_dir=Path("data/gold/kg/parquet/nodes"),
    edges_dir=Path("data/gold/kg/parquet/edges"),
    out_dir=Path("evals/outputs"),
    pagerank_upper=5,
    pagerank_lower=15,
    nodes_per_type=100,
    true_neighbors=10,
    false_neighbors=5,
    seed=42,
)
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--nodes` | `data/gold/kg/parquet/nodes` | Directory containing node parquet files |
| `--edges` | `data/gold/kg/parquet/edges` | Directory containing edge parquet files |
| `--out` | `evals/outputs` | Output directory |
| `--config` | `conf/base/evals.yml` | Path to config file |
| `--pagerank-upper` | config | Upper percentile cutoff (top X%) |
| `--pagerank-lower` | config | Lower percentile cutoff (top X%) |
| `--nodes-per-type` | config | Nodes to sample per node type |
| `--true-neighbors` | config | Max true neighbors to sample per node |
| `--false-neighbors` | config | False neighbors to sample per node |
| `--seed` | config | Random seed for reproducibility |

Parameters marked "config" are loaded from `conf/base/evals.yml` and can be overridden via CLI.

## Edge Cases

- **Nodes with fewer than 10 neighbors**: All available neighbors are sampled (tracked in summary stats)
- **Empty percentile range**: An error is raised if no nodes fall within the specified range
- **Isolated nodes**: Nodes not in the graph are skipped during edge sampling

## Related Commands

- `uv run cli pagerank` - Compute and display PageRank scores for all nodes
- `uv run cli metrics` - Generate graph metrics
