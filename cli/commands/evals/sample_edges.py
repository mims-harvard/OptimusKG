"""Edge evaluation dataset generator.

Generates evaluation datasets for edge prediction models by:
1. Selecting seed nodes — either by reading a pre-computed centrality CSV and
   sampling within a percentile range, or by uniform random sampling per type
2. Creating true/false edge pairs for evaluation
"""

from __future__ import annotations

import json
import logging
import random
from pathlib import Path
from typing import Literal

import matplotlib.pyplot as plt
import networkx as nx
import polars as pl
import yaml

from .utils import (
    CentralityMetric,
    GraphMode,
    load_graph,
    load_node_metadata,
)

logger = logging.getLogger("cli")

# Default config path
DEFAULT_CONFIG_PATH = Path("conf/base/evals.yml")

# Fallback defaults (used if config file not found)
FALLBACK_DEFAULTS = {
    "centrality_upper": 10,
    "centrality_lower": 20,
    "nodes_per_type": 100,
    "true_neighbors": 10,
    "false_neighbors": 5,
    "seed": 42,
}

EdgeSampling = Literal["uniform", "degree"]

# Valid centrality metric names (must match filenames written by `cli evals centrality`)
CENTRALITY_METRICS: frozenset[str] = frozenset(
    ["pagerank", "degree", "betweenness", "closeness", "eigenvector"]
)

# Method is either a centrality metric name or "uniform"
SamplingMetric = Literal["pagerank", "degree", "betweenness", "closeness", "eigenvector", "uniform"]


def load_config(config_path: Path | None = None) -> dict:
    """Load edge eval configuration from YAML file.

    Args:
        config_path: Path to config file. Defaults to conf/base/evals.yml.

    Returns:
        Dictionary with edge_eval configuration values.
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    if not config_path.exists():
        logger.warning(
            "Config file not found: %s, using fallback defaults", config_path
        )
        return FALLBACK_DEFAULTS.copy()

    with open(config_path) as f:
        config = yaml.safe_load(f)

    return config.get("edge_eval", FALLBACK_DEFAULTS.copy())


def load_precomputed_stratified(
    csv_path: Path,
) -> dict[str, pl.DataFrame]:
    """Load a pre-computed centrality CSV and return DataFrames stratified by node type.

    The CSV must have been produced by ``cli evals centrality`` and contain at
    least the columns ``id``, ``label``, ``name``, ``centrality``.

    Args:
        csv_path: Path to the centrality CSV (e.g. ``data/gold/evals/pagerank_undirected.csv``).

    Returns:
        Dict mapping node_type -> DataFrame with columns:
        [node_id, node_type, node_name, centrality, rank_within_type, percentile]

    Raises:
        FileNotFoundError: If ``csv_path`` does not exist.
    """
    if not csv_path.exists():
        msg = (
            f"Pre-computed centrality file not found: {csv_path}\n"
            "Run `cli evals centrality` first to generate it."
        )
        raise FileNotFoundError(msg)

    centrality_df = pl.read_csv(csv_path).rename(
        {"id": "node_id", "label": "node_type", "name": "node_name"}
    )

    stratified: dict[str, pl.DataFrame] = {}
    node_types = centrality_df["node_type"].unique().drop_nulls().to_list()

    for node_type in node_types:
        type_df = (
            centrality_df.filter(pl.col("node_type") == node_type)
            .sort("centrality", descending=True)
            .with_row_index("rank_within_type", offset=1)
        )
        n_nodes = type_df.height
        type_df = type_df.with_columns(
            ((pl.col("rank_within_type") - 1) / n_nodes * 100).alias("percentile")
        )
        stratified[node_type] = type_df

    logger.info(
        "Loaded pre-computed centrality from %s (%d node types)", csv_path, len(stratified)
    )
    return stratified


def sample_nodes_uniform(
    node_metadata: pl.DataFrame,
    nodes_per_type: int,
    seed: int,
) -> tuple[pl.DataFrame, dict]:
    """Sample nodes uniformly at random within each node type.

    No centrality computation is performed; every named node is equally
    likely to be selected.

    Args:
        node_metadata: DataFrame with columns id, label, name.
        nodes_per_type: Maximum number of nodes to sample per type.
        seed: Random seed.

    Returns:
        Tuple of (sampled DataFrame, sampling stats dict).
        The DataFrame has columns: node_id, node_type, node_name, centrality
        (centrality is null for all rows).
    """
    sampled_frames: list[pl.DataFrame] = []
    sampling_stats: dict[str, dict] = {}

    for node_type in sorted(node_metadata["label"].unique().drop_nulls().to_list()):
        eligible = node_metadata.filter(
            (pl.col("label") == node_type)
            & pl.col("name").is_not_null()
            & (pl.col("name") != "")
        ).select(
            pl.col("id").alias("node_id"),
            pl.col("label").alias("node_type"),
            pl.col("name").alias("node_name"),
            pl.lit(None).cast(pl.Float64).alias("centrality"),
        )

        n_eligible = eligible.height
        n_to_sample = min(nodes_per_type, n_eligible)

        if n_eligible > 0:
            sampled_frames.append(eligible.sample(n=n_to_sample, seed=seed))

        sampling_stats[node_type] = {
            "total_nodes": n_eligible,
            "eligible_in_range": n_eligible,
            "sampled": n_to_sample if n_eligible > 0 else 0,
            "percentile_range": None,
        }

        logger.info(
            "Type '%s': %d eligible, sampled %d (uniform)",
            node_type,
            n_eligible,
            n_to_sample if n_eligible > 0 else 0,
        )

    if not sampled_frames:
        raise ValueError("No nodes were sampled. Check node metadata.")

    result = pl.concat(sampled_frames)
    logger.info("Total uniformly sampled nodes: %d", result.height)
    return result, sampling_stats


def stratify_centrality_by_type(
    G: nx.MultiDiGraph,
    node_metadata: pl.DataFrame,
    metric: CentralityMetric = "pagerank",
    alpha: float = 0.85,
) -> dict[str, pl.DataFrame]:
    """Compute a centrality metric and return DataFrames stratified by node type.

    Args:
        G: NetworkX MultiDiGraph.
        node_metadata: DataFrame with id, label, name columns.
        metric: Centrality metric to compute.
        alpha: Damping factor (PageRank only).

    Returns:
        Dict mapping node_type -> DataFrame with columns:
        [node_id, node_type, node_name, centrality, rank_within_type, percentile]
    """
    scores = compute_centrality(G, metric=metric, alpha=alpha)
    centrality_df = centrality_to_dataframe(scores, node_metadata).rename(
        {"id": "node_id", "label": "node_type", "name": "node_name"}
    )

    # Stratify by node type
    stratified: dict[str, pl.DataFrame] = {}
    node_types = centrality_df["node_type"].unique().drop_nulls().to_list()

    for node_type in node_types:
        type_df = (
            centrality_df.filter(pl.col("node_type") == node_type)
            .sort("centrality", descending=True)
            .with_row_index("rank_within_type", offset=1)
        )
        # Add percentile (rank 1 = 0th percentile, i.e., top node)
        n_nodes = type_df.height
        type_df = type_df.with_columns(
            ((pl.col("rank_within_type") - 1) / n_nodes * 100).alias("percentile")
        )
        stratified[node_type] = type_df

    logger.info("Stratified nodes into %s types", len(stratified))
    return stratified


def plot_centrality_distributions(
    stratified_data: dict[str, pl.DataFrame],
    metric: CentralityMetric,
    out_path: Path,
) -> None:
    """Create a multi-panel figure showing centrality score vs rank for each node type."""
    n_types = len(stratified_data)
    n_cols = 5
    n_rows = (n_types + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 3 * n_rows))
    axes = axes.flatten() if n_types > 1 else [axes]

    sorted_types = sorted(stratified_data.keys())
    metric_label = metric.replace("_", " ").title()

    for idx, node_type in enumerate(sorted_types):
        ax = axes[idx]
        df = stratified_data[node_type]

        ranks = df["rank_within_type"].to_numpy()
        scores = df["centrality"].to_numpy()

        ax.plot(ranks, scores, linewidth=0.8, color=plt.cm.tab10(idx % 10))
        ax.set_xlabel("Rank", fontsize=8)
        ax.set_ylabel(metric_label, fontsize=8)
        ax.set_title(f"{node_type}\n(n={len(ranks):,})", fontsize=9, fontweight="bold")
        ax.tick_params(axis="both", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Hide unused subplots
    for idx in range(n_types, len(axes)):
        axes[idx].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved centrality distribution plot to %s", out_path)


def sample_nodes_in_percentile(
    stratified_data: dict[str, pl.DataFrame],
    upper_percentile: float,
    lower_percentile: float,
    nodes_per_type: int,
    seed: int,
) -> tuple[pl.DataFrame, dict]:
    """Sample nodes from the specified percentile range for each node type.

    Args:
        stratified_data: Dict mapping node_type -> DataFrame with centrality data
        upper_percentile: Top X% cutoff (e.g., 5 means top 5%)
        lower_percentile: Bottom cutoff (e.g., 15 means top 15%)
        nodes_per_type: Number of nodes to sample per type
        seed: Random seed

    Returns:
        Tuple of (sampled DataFrame, sampling stats dict)
    """
    random.seed(seed)
    sampled_frames: list[pl.DataFrame] = []
    sampling_stats: dict[str, dict] = {}

    for node_type, df in stratified_data.items():
        # Filter to percentile range and exclude nodes with blank names
        eligible = df.filter(
            (pl.col("percentile") >= upper_percentile)
            & (pl.col("percentile") <= lower_percentile)
            & pl.col("node_name").is_not_null()
            & (pl.col("node_name") != "")
        )

        n_eligible = eligible.height
        n_to_sample = min(nodes_per_type, n_eligible)

        if n_eligible > 0:
            sampled = eligible.sample(n=n_to_sample, seed=seed)
            sampled_frames.append(sampled)

        sampling_stats[node_type] = {
            "total_nodes": df.height,
            "eligible_in_range": n_eligible,
            "sampled": n_to_sample if n_eligible > 0 else 0,
            "percentile_range": [upper_percentile, lower_percentile],
        }

        logger.info(
            "Type '%s': %d eligible, sampled %d",
            node_type,
            n_eligible,
            n_to_sample if n_eligible > 0 else 0,
        )

    if not sampled_frames:
        raise ValueError("No nodes were sampled. Check percentile parameters.")

    result = pl.concat(sampled_frames)
    logger.info("Total sampled nodes: %d", result.height)
    return result, sampling_stats


def sample_edges_for_nodes(
    sampled_nodes: pl.DataFrame,
    G: nx.MultiDiGraph,
    node_metadata: pl.DataFrame,
    true_neighbors_per_node: int,
    false_neighbors_per_node: int,
    seed: int,
    edge_sampling: EdgeSampling = "uniform",
) -> tuple[pl.DataFrame, dict]:
    """Sample true and false edges for each sampled node.

    Args:
        sampled_nodes: DataFrame of sampled seed nodes.
        G: NetworkX MultiDiGraph with ``label`` stored on every edge.
        node_metadata: DataFrame with node id, label, name.
        true_neighbors_per_node: Max true neighbors to sample per node.
        false_neighbors_per_node: False neighbors to sample per node.
        seed: Random seed.
        edge_sampling: Strategy for sampling true and false neighbor candidates.
            - ``"uniform"`` (default): every candidate is equally likely.
            - ``"degree"``: candidates are weighted by their total degree
              (in + out) so higher-degree nodes are proportionally more likely
              to be selected. This mimics a preferential-attachment prior and
              tends to produce harder negatives for link-prediction evaluation.

    Returns:
        Tuple of (edges DataFrame, edge sampling stats dict)
    """
    random.seed(seed)

    # Create lookup for node metadata
    metadata_dict = {
        row["id"]: {"type": row["label"], "name": row["name"]}
        for row in node_metadata.iter_rows(named=True)
    }

    # Create set of nodes with valid (non-blank) names
    nodes_with_names = {
        node_id
        for node_id, meta in metadata_dict.items()
        if meta["name"] is not None and meta["name"] != ""
    }

    all_nodes = set(G.nodes())
    edge_records: list[dict] = []

    # Pre-compute degree weights once if needed (total degree = in + out)
    if edge_sampling == "degree":
        degree_weight: dict[str, float] = {
            node: G.in_degree(node) + G.out_degree(node)
            for node in all_nodes
        }
    else:
        degree_weight = {}

    def _sample(population: list[str], k: int) -> list[str]:
        """Sample up to k unique items, optionally weighted by degree."""
        if k <= 0 or not population:
            return []
        k = min(k, len(population))
        if edge_sampling == "degree":
            weights = [degree_weight.get(n, 0) for n in population]
            total = sum(weights)
            if total == 0:
                # All weights are zero — fall back to uniform
                return random.sample(population, k)
            # random.choices samples with replacement; loop until we have k unique items
            chosen: set[str] = set()
            while len(chosen) < k:
                chosen.update(random.choices(population, weights=weights, k=k - len(chosen)))
            return list(chosen)[:k]
        return random.sample(population, k)

    # Track stats by seed node type
    type_stats: dict[str, dict] = {}

    for row in sampled_nodes.iter_rows(named=True):
        seed_id = row["node_id"]
        seed_type = row["node_type"]
        seed_name = row["node_name"]
        seed_centrality = row["centrality"]

        # Initialize type stats
        if seed_type not in type_stats:
            type_stats[seed_type] = {
                "true_edges": 0,
                "false_edges": 0,
                "total_neighbors": 0,
                "nodes_with_few_neighbors": 0,
            }

        # Neighbors = successors ∪ predecessors (full undirected view of connectivity)
        if seed_id in G:
            neighbors = set(G.successors(seed_id)) | set(G.predecessors(seed_id))
        else:
            neighbors = set()

        type_stats[seed_type]["total_neighbors"] += len(neighbors)

        # Sample true neighbors (only those with valid names; exclude self-loops)
        neighbors_list = [n for n in neighbors if n in nodes_with_names and n != seed_id]
        n_true = min(true_neighbors_per_node, len(neighbors_list))
        if n_true < true_neighbors_per_node:
            type_stats[seed_type]["nodes_with_few_neighbors"] += 1

        true_sample = _sample(neighbors_list, n_true)

        for target_id in true_sample:
            target_meta = metadata_dict.get(target_id, {"type": None, "name": None})
            # Collect relation labels from whichever directed arc(s) exist.
            # neighbors is a union of successors and predecessors, so the edge
            # may be stored as seed→target, target→seed, or both (undirected
            # edges in the data produce arcs in both directions).
            relation_type = "|".join(
                sorted({
                    edata["label"]
                    for (u, v) in ((seed_id, target_id), (target_id, seed_id))
                    if G.has_edge(u, v)
                    for edata in G[u][v].values()
                    if "label" in edata
                })
            ) or None
            edge_records.append(
                {
                    "seed_node_id": seed_id,
                    "seed_node_type": seed_type,
                    "seed_node_name": seed_name,
                    "seed_centrality": seed_centrality,
                    "target_node_id": target_id,
                    "target_node_type": target_meta["type"],
                    "target_node_name": target_meta["name"],
                    "is_true_edge": True,
                    "relation_type": relation_type,
                }
            )
            type_stats[seed_type]["true_edges"] += 1

        # Sample false neighbors (non-edges, only those with valid names)
        non_neighbors = all_nodes - neighbors - {seed_id}
        non_neighbors_list = [n for n in non_neighbors if n in nodes_with_names]
        false_sample = _sample(non_neighbors_list, false_neighbors_per_node)

        for target_id in false_sample:
            target_meta = metadata_dict.get(target_id, {"type": None, "name": None})
            edge_records.append(
                {
                    "seed_node_id": seed_id,
                    "seed_node_type": seed_type,
                    "seed_node_name": seed_name,
                    "seed_centrality": seed_centrality,
                    "target_node_id": target_id,
                    "target_node_type": target_meta["type"],
                    "target_node_name": target_meta["name"],
                    "is_true_edge": False,
                    "relation_type": None,
                }
            )
            type_stats[seed_type]["false_edges"] += 1

    edges_df = pl.DataFrame(edge_records)

    # Compute summary stats
    total_true = sum(s["true_edges"] for s in type_stats.values())
    total_false = sum(s["false_edges"] for s in type_stats.values())
    total_neighbors = sum(s["total_neighbors"] for s in type_stats.values())
    nodes_with_few = sum(s["nodes_with_few_neighbors"] for s in type_stats.values())

    edge_stats = {
        "total_true_edges": total_true,
        "total_false_edges": total_false,
        "total_edges": total_true + total_false,
        "nodes_with_fewer_than_max_neighbors": nodes_with_few,
        "average_neighbors_per_seed": total_neighbors / sampled_nodes.height
        if sampled_nodes.height > 0
        else 0,
        "by_seed_type": {
            k: {
                "true_edges": v["true_edges"],
                "false_edges": v["false_edges"],
                "avg_neighbors": v["total_neighbors"]
                / sampled_nodes.filter(pl.col("node_type") == k).height
                if sampled_nodes.filter(pl.col("node_type") == k).height > 0
                else 0,
            }
            for k, v in type_stats.items()
        },
    }

    logger.info(
        "Sampled %d true edges and %d false edges (edge_sampling=%s)",
        total_true,
        total_false,
        edge_sampling,
    )
    return edges_df, edge_stats


def run(
    nodes_path: Path,
    edges_path: Path,
    out_dir: Path,
    metric: SamplingMetric = "pagerank",
    centrality_upper: int | None = None,
    centrality_lower: int | None = None,
    nodes_per_type: int | None = None,
    true_neighbors: int | None = None,
    false_neighbors: int | None = None,
    seed: int | None = None,
    config_path: Path | None = None,
    graph_mode: GraphMode = "undirected",
    edge_sampling: EdgeSampling = "uniform",
) -> None:
    """Run edge evaluation dataset generation.

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.
        out_dir: Output directory for generated files.
        metric: Node-selection strategy. Either a centrality metric name
            ("pagerank", "degree", "betweenness", "closeness", "eigenvector")
            or "uniform".
            - Centrality metric: reads the pre-computed CSV
              ``<out_dir>/<metric>_<graph_mode>.csv`` produced by
              ``cli evals centrality``. Raises FileNotFoundError if missing.
              Nodes are then sampled within the [centrality_upper, centrality_lower]
              percentile band.
            - "uniform": samples nodes uniformly at random within each node type;
              centrality percentile parameters are ignored.
        centrality_upper: Upper percentile cutoff (top X%). Ignored when
            metric="uniform". Overrides config.
        centrality_lower: Lower percentile cutoff (top X%). Ignored when
            metric="uniform". Overrides config.
        nodes_per_type: Nodes to sample per type. Overrides config.
        true_neighbors: True neighbors per node. Overrides config.
        false_neighbors: False neighbors per node. Overrides config.
        seed: Random seed. Overrides config.
        config_path: Path to config file. Defaults to conf/base/evals.yml.
        graph_mode: Edge directionality passed to load_graph.
            "undirected" (default) adds reverse arcs for every edge;
            "directed" only adds reverse arcs where undirected=true.
        edge_sampling: Strategy for sampling neighbor candidates.
            "uniform" (default): uniform random sampling.
            "degree": weight candidates by total degree so higher-degree
            nodes are proportionally more likely to be sampled.
    """
    # Load config and merge with CLI args (CLI takes precedence)
    config = load_config(config_path)
    centrality_upper = (
        centrality_upper if centrality_upper is not None else config["centrality_upper"]
    )
    centrality_lower = (
        centrality_lower if centrality_lower is not None else config["centrality_lower"]
    )
    nodes_per_type = (
        nodes_per_type if nodes_per_type is not None else config["nodes_per_type"]
    )
    true_neighbors = (
        true_neighbors if true_neighbors is not None else config["true_neighbors"]
    )
    false_neighbors = (
        false_neighbors if false_neighbors is not None else config["false_neighbors"]
    )
    seed = seed if seed is not None else config["seed"]

    out_dir.mkdir(parents=True, exist_ok=True)

    # Load graph (always needed for edge sampling)
    logger.info("Loading graph...")
    G, node_types, edge_types = load_graph(nodes_path, edges_path, graph_mode=graph_mode)

    # --- Node sampling --------------------------------------------------
    if metric == "uniform":
        logger.info("Method=uniform: sampling nodes uniformly per node type...")
        node_metadata = load_node_metadata(nodes_path)
        sampled_nodes, node_sampling_stats = sample_nodes_uniform(
            node_metadata,
            nodes_per_type=nodes_per_type,
            seed=seed,
        )
    else:
        # metric is a centrality metric name — read the pre-computed CSV
        csv_path = out_dir / f"{metric}_{graph_mode}.csv"
        logger.info("Method=%s: loading pre-computed centrality from %s...", metric, csv_path)
        stratified_data = load_precomputed_stratified(csv_path)

        # Plot centrality distributions
        logger.info("Generating centrality distribution plots...")
        plot_centrality_distributions(
            stratified_data,
            metric,
            out_dir / f"{metric}_{graph_mode}_distribution_by_type.pdf",
        )

        # Sample nodes within percentile band
        logger.info(
            "Sampling nodes in percentile range [%d%%, %d%%]...",
            centrality_upper,
            centrality_lower,
        )
        sampled_nodes, node_sampling_stats = sample_nodes_in_percentile(
            stratified_data,
            upper_percentile=centrality_upper,
            lower_percentile=centrality_lower,
            nodes_per_type=nodes_per_type,
            seed=seed,
        )
        node_metadata = load_node_metadata(nodes_path)

    # Save sampled nodes (sorted by node_type, centrality desc)
    if metric == "uniform":
        nodes_stem = "sampled_nodes_uniform"
    else:
        nodes_stem = f"sampled_nodes_{metric}_lower={centrality_lower}_upper={centrality_upper}"
    sampled_nodes_path = out_dir / f"{nodes_stem}.csv"
    sampled_nodes.sort("node_type", "centrality", descending=[False, True]).write_csv(
        sampled_nodes_path
    )
    logger.info("Saved sampled nodes to %s", sampled_nodes_path)

    # Sample edges
    logger.info(
        "Sampling edges for %d seed nodes (edge_sampling=%s)...",
        sampled_nodes.height,
        edge_sampling,
    )
    sampled_edges, edge_sampling_stats = sample_edges_for_nodes(
        sampled_nodes,
        G,
        node_metadata,
        true_neighbors_per_node=true_neighbors,
        false_neighbors_per_node=false_neighbors,
        seed=seed,
        edge_sampling=edge_sampling,
    )

    # Save sampled edges (sorted by seed_node_type, seed_centrality desc)
    edges_stem = f"sampled_edges_{metric}_true={true_neighbors}_false={false_neighbors}"
    sampled_edges_path = out_dir / f"{edges_stem}.csv"
    sampled_edges.sort(
        "seed_node_type", "seed_centrality", descending=[False, True]
    ).write_csv(sampled_edges_path)
    logger.info("Saved sampled edges to %s", sampled_edges_path)

    # Compile and save summary stats
    summary_stats = {
        "parameters": {
            "metric": metric,
            "graph_mode": graph_mode,
            "edge_sampling": edge_sampling,
            "centrality_upper_percentile": centrality_upper if metric != "uniform" else None,
            "centrality_lower_percentile": centrality_lower if metric != "uniform" else None,
            "nodes_per_type": nodes_per_type,
            "true_neighbors_per_node": true_neighbors,
            "false_neighbors_per_node": false_neighbors,
            "random_seed": seed,
        },
        "graph": {
            "total_nodes": G.number_of_nodes(),
            "total_edges": G.number_of_edges(),
            "node_types": node_types,
            "edge_types": edge_types,
        },
        "node_sampling": {
            "total_sampled_nodes": sampled_nodes.height,
            "by_type": node_sampling_stats,
        },
        "edge_sampling": edge_sampling_stats,
    }

    summary_path = out_dir / "summary_stats.json"
    with open(summary_path, "w") as f:
        json.dump(summary_stats, f, indent=2)
    logger.info("Saved summary stats to %s", summary_path)

    percentile_str = (
        f"[{centrality_upper}%%, {centrality_lower}%%]" if metric != "uniform" else "N/A (uniform)"
    )
    logger.info(
        "Analysis complete\n"
        "Parameters:\n"
        "  - Method: %s\n"
        "  - Graph mode: %s\n"
        "  - Edge sampling: %s\n"
        "  - Centrality percentile range: %s\n"
        "  - Nodes per type: %d\n"
        "  - True neighbors per node: %d\n"
        "  - False neighbors per node: %d\n"
        "  - Random seed: %d\n"
        "Results:\n"
        "  - Sampled nodes: %d\n"
        "  - True edges: %d\n"
        "  - False edges: %d\n"
        "  - Total edge pairs: %d\n"
        "Output files:\n"
        "  - %s\n"
        "  - %s\n"
        "  - %s",
        metric,
        graph_mode,
        edge_sampling,
        percentile_str,
        nodes_per_type,
        true_neighbors,
        false_neighbors,
        seed,
        sampled_nodes.height,
        edge_sampling_stats["total_true_edges"],
        edge_sampling_stats["total_false_edges"],
        edge_sampling_stats["total_edges"],
        sampled_nodes_path,
        sampled_edges_path,
        out_dir / "summary_stats.json",
    )
