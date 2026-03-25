"""Edge evaluation dataset generator.

Generates evaluation datasets for edge prediction models by:
1. Ranking nodes by PageRank within each node type
2. Sampling nodes from a specific percentile range
3. Creating true/false edge pairs for evaluation
"""

from __future__ import annotations

import json
import logging
import random
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import polars as pl
import yaml

from .utils import (
    compute_pagerank,
    load_graph,
    load_node_metadata,
    pagerank_to_dataframe,
)

logger = logging.getLogger("cli")

# Default config path
DEFAULT_CONFIG_PATH = Path("conf/base/evals.yml")

# Fallback defaults (used if config file not found)
FALLBACK_DEFAULTS = {
    "pagerank_upper": 10,
    "pagerank_lower": 20,
    "nodes_per_type": 100,
    "true_neighbors": 10,
    "false_neighbors": 5,
    "seed": 42,
}


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


def stratify_pagerank_by_type(
    G: nx.MultiDiGraph, node_metadata: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    """Compute PageRank and return DataFrames stratified by node type.

    Args:
        G: NetworkX graph.
        node_metadata: DataFrame with id, label, name columns.

    Returns:
        Dict mapping node_type -> DataFrame with columns:
        [node_id, node_type, node_name, pagerank, rank_within_type, percentile]
    """
    # Use shared PageRank computation
    scores = compute_pagerank(G, alpha=0.85)
    pagerank_df = pagerank_to_dataframe(scores, node_metadata).rename(
        {"id": "node_id", "label": "node_type", "name": "node_name"}
    )

    # Stratify by node type
    stratified: dict[str, pl.DataFrame] = {}
    node_types = pagerank_df["node_type"].unique().drop_nulls().to_list()

    for node_type in node_types:
        type_df = (
            pagerank_df.filter(pl.col("node_type") == node_type)
            .sort("pagerank", descending=True)
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


def plot_pagerank_distributions(
    stratified_data: dict[str, pl.DataFrame], out_path: Path
) -> None:
    """Create a multi-panel figure showing PageRank vs Rank for each node type."""
    n_types = len(stratified_data)
    n_cols = 5
    n_rows = (n_types + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 3 * n_rows))
    axes = axes.flatten() if n_types > 1 else [axes]

    sorted_types = sorted(stratified_data.keys())

    for idx, node_type in enumerate(sorted_types):
        ax = axes[idx]
        df = stratified_data[node_type]

        ranks = df["rank_within_type"].to_numpy()
        pageranks = df["pagerank"].to_numpy()

        ax.plot(ranks, pageranks, linewidth=0.8, color=plt.cm.tab10(idx % 10))
        # ax.set_yscale("log")
        # ax.set_xscale("log")
        ax.set_xlabel("Rank", fontsize=8)
        ax.set_ylabel("PageRank", fontsize=8)
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
    logger.info("Saved PageRank distribution plot to %s", out_path)


def sample_nodes_in_percentile(
    stratified_data: dict[str, pl.DataFrame],
    upper_percentile: float,
    lower_percentile: float,
    nodes_per_type: int,
    seed: int,
) -> tuple[pl.DataFrame, dict]:
    """Sample nodes from the specified percentile range for each node type.

    Args:
        stratified_data: Dict mapping node_type -> DataFrame with pagerank data
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
            # Sample randomly
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
) -> tuple[pl.DataFrame, dict]:
    """Sample true and false edges for each sampled node.

    Args:
        sampled_nodes: DataFrame of sampled seed nodes
        G: NetworkX MultiDiGraph with ``label`` stored on every edge
        node_metadata: DataFrame with node id, label, name
        true_neighbors_per_node: Max true neighbors to sample per node
        false_neighbors_per_node: False neighbors to sample per node
        seed: Random seed

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

    # Track stats by seed node type
    type_stats: dict[str, dict] = {}

    for row in sampled_nodes.iter_rows(named=True):
        seed_id = row["node_id"]
        seed_type = row["node_type"]
        seed_name = row["node_name"]
        seed_pagerank = row["pagerank"]

        # Initialize type stats
        if seed_type not in type_stats:
            type_stats[seed_type] = {
                "true_edges": 0,
                "false_edges": 0,
                "total_neighbors": 0,
                "nodes_with_few_neighbors": 0,
            }

        # Get neighbors
        if seed_id in G:
            # neighbors = set(G.neighbors(seed_id))
            neighbors = set(G.successors(seed_id)) | set(G.predecessors(seed_id))
        else:
            neighbors = set()

        type_stats[seed_type]["total_neighbors"] += len(neighbors)

        # Sample true neighbors (only those with valid names)
        neighbors_list = [n for n in neighbors if n in nodes_with_names]
        n_true = min(true_neighbors_per_node, len(neighbors_list))
        if n_true < true_neighbors_per_node:
            type_stats[seed_type]["nodes_with_few_neighbors"] += 1

        true_sample = random.sample(neighbors_list, n_true) if n_true > 0 else []

        for target_id in true_sample:
            target_meta = metadata_dict.get(target_id, {"type": None, "name": None})
            edge_records.append(
                {
                    "seed_node_id": seed_id,
                    "seed_node_type": seed_type,
                    "seed_node_name": seed_name,
                    "seed_pagerank": seed_pagerank,
                    "target_node_id": target_id,
                    "target_node_type": target_meta["type"],
                    "target_node_name": target_meta["name"],
                    "is_true_edge": True,
                    "relation_type": "|".join(
                        sorted({
                            edata["label"]
                            for edata in G[seed_id][target_id].values()
                            if "label" in edata
                        })
                    ) or None,
                }
            )
            type_stats[seed_type]["true_edges"] += 1

        # Sample false neighbors (non-edges, only those with valid names)
        non_neighbors = all_nodes - neighbors - {seed_id}
        non_neighbors_list = [n for n in non_neighbors if n in nodes_with_names]
        n_false = min(false_neighbors_per_node, len(non_neighbors_list))
        false_sample = random.sample(non_neighbors_list, n_false) if n_false > 0 else []

        for target_id in false_sample:
            target_meta = metadata_dict.get(target_id, {"type": None, "name": None})
            edge_records.append(
                {
                    "seed_node_id": seed_id,
                    "seed_node_type": seed_type,
                    "seed_node_name": seed_name,
                    "seed_pagerank": seed_pagerank,
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
        "Sampled %d true edges and %d false edges",
        total_true,
        total_false,
    )
    return edges_df, edge_stats


def run(
    nodes_path: Path,
    edges_path: Path,
    out_dir: Path,
    pagerank_upper: int | None = None,
    pagerank_lower: int | None = None,
    nodes_per_type: int | None = None,
    true_neighbors: int | None = None,
    false_neighbors: int | None = None,
    seed: int | None = None,
    config_path: Path | None = None,
) -> None:
    """Run edge evaluation dataset generation.

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.
        out_dir: Output directory for generated files
        pagerank_upper: Upper percentile cutoff (top X%). Overrides config.
        pagerank_lower: Lower percentile cutoff (top X%). Overrides config.
        nodes_per_type: Nodes to sample per type. Overrides config.
        true_neighbors: True neighbors per node. Overrides config.
        false_neighbors: False neighbors per node. Overrides config.
        seed: Random seed. Overrides config.
        config_path: Path to config file. Defaults to conf/base/evals.yml.
    """
    # Load config and merge with CLI args (CLI takes precedence)
    config = load_config(config_path)
    pagerank_upper = (
        pagerank_upper if pagerank_upper is not None else config["pagerank_upper"]
    )
    pagerank_lower = (
        pagerank_lower if pagerank_lower is not None else config["pagerank_lower"]
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

    # Load data
    logger.info("Loading graph and node metadata...")
    G, node_types, edge_types = load_graph(nodes_path, edges_path)
    node_metadata = load_node_metadata(nodes_path)

    # Compute PageRank stratified by type
    stratified_data = stratify_pagerank_by_type(G, node_metadata)

    # Plot PageRank distributions
    logger.info("Generating PageRank distribution plots...")
    plot_pagerank_distributions(
        stratified_data, out_dir / "pagerank_distribution_by_type.pdf"
    )

    # Sample nodes
    logger.info(
        "Sampling nodes in percentile range [%d%%, %d%%]...",
        pagerank_upper,
        pagerank_lower,
    )
    sampled_nodes, node_sampling_stats = sample_nodes_in_percentile(
        stratified_data,
        upper_percentile=pagerank_upper,
        lower_percentile=pagerank_lower,
        nodes_per_type=nodes_per_type,
        seed=seed,
    )

    # Save sampled nodes (sorted by node_type, pagerank desc)
    sampled_nodes_path = out_dir / "sampled_nodes.csv"
    sampled_nodes.sort("node_type", "pagerank", descending=[False, True]).write_csv(
        sampled_nodes_path
    )
    logger.info("Saved sampled nodes to %s", sampled_nodes_path)

    # Sample edges
    logger.info("Sampling edges for %d seed nodes...", sampled_nodes.height)
    sampled_edges, edge_sampling_stats = sample_edges_for_nodes(
        sampled_nodes,
        G,
        node_metadata,
        true_neighbors_per_node=true_neighbors,
        false_neighbors_per_node=false_neighbors,
        seed=seed,
    )

    # Save sampled edges (sorted by seed_node_type, seed_pagerank desc)
    sampled_edges_path = out_dir / "sampled_edges.csv"
    sampled_edges.sort(
        "seed_node_type", "seed_pagerank", descending=[False, True]
    ).write_csv(sampled_edges_path)
    logger.info("Saved sampled edges to %s", sampled_edges_path)

    # Compile and save summary stats
    summary_stats = {
        "parameters": {
            "pagerank_upper_percentile": pagerank_upper,
            "pagerank_lower_percentile": pagerank_lower,
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

    # Log summary to console
    logger.info(
        "Analysis complete\n"
        "Parameters:\n"
        "  - PageRank percentile range: [%d%%, %d%%]\n"
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
        "  - %s\n"
        "  - %s",
        pagerank_upper,
        pagerank_lower,
        nodes_per_type,
        true_neighbors,
        false_neighbors,
        seed,
        sampled_nodes.height,
        edge_sampling_stats["total_true_edges"],
        edge_sampling_stats["total_false_edges"],
        edge_sampling_stats["total_edges"],
        out_dir / "pagerank_distribution_by_type.pdf",
        out_dir / "sampled_nodes.csv",
        out_dir / "sampled_edges.csv",
        out_dir / "summary_stats.json",
    )