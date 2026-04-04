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

### `uv run cli evals paperqa`

Evaluate sampled edges using PaperQA3 via the Edison client. Operates in two phases:

1. **`submit`** — Constructs literature-search prompts from a sampled-edges file and submits them as async jobs to the Edison platform.
2. **`poll`** — Checks job status for a previous submission, downloads completed results, and optionally logs to W&B.

Both phases share an alphanumeric run ID (a `YYYYMMDD_HHMMSS` timestamp generated at submit time) so that outputs are linked:

| Phase | Input file | Output file |
|-------|-----------|-------------|
| submit | `sampled_edges_*.csv` | `<run_id>_submitted_edges.csv` |
| poll | `<run_id>_submitted_edges.csv` | `<run_id>_polled_edges.csv` |

The command validates that the input file matches the expected naming convention for the chosen action and raises an error otherwise. To plot rating distributions from a completed poll, use **`evals paperqa-figures`** (see below).

**Usage:**
```bash
# Submit jobs (input must be a sampled_edges file)
uv run cli evals paperqa --action submit --input data/gold/evals/sampled_edges_degree_true=10_false=5.csv

# Poll for results (input must be the submitted_edges file from submit)
uv run cli evals paperqa --action poll --input data/gold/evals/20260328_163632_submitted_edges.csv

# Pilot run with a small subset
uv run cli evals paperqa --action submit --limit 10

# Log results to W&B on poll
uv run cli evals paperqa --action poll --input data/gold/evals/20260328_163632_submitted_edges.csv --wandb-project my-project
```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--input` | `data/gold/evals/sampled_edges_degree_true=10_false=1.csv` | Input CSV (sampled\_edges for submit, submitted\_edges for poll) |
| `--out` | `data/gold/evals` | Output directory |
| `--action` | `submit` | Phase to execute: `submit` or `poll` |
| `--limit` | None | Limit number of edges to evaluate (useful for pilots) |
| `--wandb-project` | None | W&B project name for logging (poll only) |
| `--api-min-interval` | `2.0` | Minimum seconds between Edison API calls |
| `--max-rate-limit-attempts` | `15` | Max retries with exponential backoff on 429/5xx |

**Environment variables:** Requires `EDISON_API_KEY` to be set (loaded from `.env` at the project root).

---

### `uv run cli evals paperqa-figures`

Generate bar plots from a **polled** PaperQA3 run (after `paperqa --action poll` has produced ratings).

**Inputs:** A `<run_id>_polled_edges.csv` file with at least `seed_node_type`, `is_true_edge`, `rating`, and `relation_type` (rows without a parseable rating are dropped).

**Outputs** (written to `--out`, or the same directory as `--input` if omitted):

| File | Description |
|------|-------------|
| `<stem>_barplot.pdf` | Two panels (false edges \| true edges): rating on the x-axis, count on the y-axis, bars stacked by seed node type (types ordered by prevalence). |
| `<stem>_grouped_barplot.pdf` | Faceted by seed node type: true edges stacked by relation type; false edges as a single gray segment; ratings 1–5 on the x-axis. |
| `<stem>_grouped_barplot.svg` | Same as the grouped bar plot, SVG format. |

`<stem>` is the input filename without extension (e.g. `20260328_195935_polled_edges` for `20260328_195935_polled_edges.csv`).

The command also prints summary statistics to the console (counts and evidence rates by seed type).

**Usage:**
```bash
uv run cli evals paperqa-figures --input data/gold/evals/20260328_195935_polled_edges.csv

uv run cli evals paperqa-figures --input data/gold/evals/20260328_195935_polled_edges.csv --out figures/
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--input` | (required) | Path to the polled-edges CSV from `paperqa --action poll` |
| `--out` | parent of `--input` | Directory for PDF/SVG figures |

---

## Configuration

Parameters for `sample-edges` are loaded from `conf/base/evals.yml`. Legacy `pagerank_upper` / `pagerank_lower` keys are still accepted.

```yaml
edge_eval:
  nodes_dir: data/gold/kg/parquet/nodes
  edges_dir: data/gold/kg/parquet/edges
  out_dir: data/gold/evals
  centrality_upper: 10     # Top 10% cutoff
  centrality_lower: 90     # Top 90% cutoff
  nodes_per_type: 100
  true_neighbors: 10
  false_neighbors: 1
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
